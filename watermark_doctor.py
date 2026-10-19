"""A deterministic teaching model for partition watermarks and tumbling windows.

This is deliberately a small model, not an Apache Flink runtime emulator.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


LABELS = {
    "en": {
        "partition idle": "partition idle",
        "partition resumed": "partition resumed",
        "too late": "too late",
        "late update": "late update",
        "accepted": "accepted",
        "invalid": "Invalid parameters, event or declared partition.",
    },
    "fr": {
        "partition idle": "partition inactive",
        "partition resumed": "partition reprise",
        "too late": "trop tard",
        "late update": "mise à jour tardive",
        "accepted": "accepté",
        "invalid": "Paramètres, événement ou partition déclarée invalides.",
    },
    "es": {
        "partition idle": "partición inactiva",
        "partition resumed": "partición reanudada",
        "too late": "demasiado tarde",
        "late update": "actualización tardía",
        "accepted": "aceptado",
        "invalid": "Parámetros, evento o partición declarada inválidos.",
    },
}


def invalid():
    return ValueError(
        " / ".join(LABELS[lang]["invalid"] for lang in ("fr", "en", "es"))
    )


def validate_event(event):
    if (
        not isinstance(event, dict)
        or not isinstance(event.get("partition"), str)
        or not event["partition"].strip()
    ):
        raise invalid()
    kind = event.get("type", "event")
    if kind not in ("event", "idle", "resume"):
        raise invalid()
    if kind == "event" and (
        isinstance(event.get("ts"), bool) or not isinstance(event.get("ts"), int)
    ):
        raise invalid()


def load_events(path):
    events = []
    for line_number, raw in enumerate(
        Path(path).read_text(encoding="utf-8").splitlines(), 1
    ):
        if not raw.strip():
            continue
        try:
            event = json.loads(raw)
            validate_event(event)
        except (json.JSONDecodeError, ValueError) as error:
            raise ValueError(f"{path}:{line_number}: {error}") from error
        events.append(event)
    return events


def simulate(
    events, window, out_of_order, allowed_lateness, *, partitions=None, lang="en"
):
    if (
        lang not in LABELS
        or any(
            isinstance(v, bool) or not isinstance(v, int)
            for v in (window, out_of_order, allowed_lateness)
        )
        or window <= 0
        or out_of_order < 0
        or allowed_lateness < 0
    ):
        raise invalid()
    declared = partitions is not None
    if declared and (
        not isinstance(partitions, (list, tuple))
        or not partitions
        or any(not isinstance(p, str) or not p.strip() for p in partitions)
        or len(set(partitions)) != len(partitions)
    ):
        raise invalid()
    known = set(partitions or ())
    events = list(events)
    for event in events:
        validate_event(event)
        if declared and event["partition"] not in known:
            raise invalid()
    maxima = {}
    idle = set()
    counts = defaultdict(int)
    fired = set()
    watermark = None
    timeline = []
    for number, event in enumerate(events, 1):
        partition = event["partition"]
        kind = event.get("type", "event")
        known.add(partition)
        record = {
            "step": number,
            "partition": partition,
            "type": kind,
            "watermark_before": watermark,
            "late_updates": [],
        }
        if kind == "idle":
            idle.add(partition)
            record["result"] = "partition idle"
        elif kind == "resume":
            idle.discard(partition)
            record["result"] = "partition resumed"
        else:
            timestamp = event["ts"]
            idle.discard(partition)
            end = (timestamp // window + 1) * window
            record.update(timestamp=timestamp, window_end=end)
            if watermark is not None and watermark >= end + allowed_lateness:
                record["result"] = "too late"
            else:
                counts[end] += 1
                record["result"] = "late update" if end in fired else "accepted"
                if end in fired:
                    record["late_updates"].append({"window_end": end, "count": counts[end]})
            maxima[partition] = max(timestamp, maxima.get(partition, timestamp))
        active = [
            max_ts - out_of_order for name, max_ts in maxima.items() if name not in idle
        ]
        waiting = sorted(
            name for name in known if name not in maxima and name not in idle
        )
        if active and not waiting:
            candidate = min(active)
            watermark = candidate if watermark is None else max(watermark, candidate)
        candidates = {name: ts - out_of_order for name, ts in maxima.items() if name not in idle}
        minimum = min(candidates.values()) if candidates else None
        record["partition_candidates"] = dict(sorted(candidates.items()))
        record["limiting_partitions"] = sorted(name for name, value in candidates.items() if value == minimum)
        record["watermark_advanced"] = watermark is not None and (record["watermark_before"] is None or watermark > record["watermark_before"])
        reason = "waiting" if waiting else "idle" if not candidates else "advanced" if record["watermark_advanced"] else "held"
        explanations = {
            "fr": {"waiting": "Partition déclarée sans horodatage : progression bloquée.", "idle": "Toutes les partitions connues sont inactives : watermark conservé.", "advanced": "Le minimum des partitions actives a fait progresser le watermark.", "held": "Le minimum actif ne dépasse pas le watermark conservé."},
            "en": {"waiting": "A declared partition has no timestamp: progress is blocked.", "idle": "All known partitions are idle: watermark is retained.", "advanced": "The active-partition minimum advanced the watermark.", "held": "The active minimum does not exceed the retained watermark."},
            "es": {"waiting": "Una partición declarada no tiene tiempo: el avance está bloqueado.", "idle": "Todas las particiones conocidas están inactivas: se conserva el watermark.", "advanced": "El mínimo de particiones activas adelantó el watermark.", "held": "El mínimo activo no supera el watermark conservado."},
        }
        record["progress_reason"] = reason
        record["explanation"] = explanations[lang][reason]
        record["watermark_after"] = watermark
        record["active_partitions"] = sorted(name for name in known if name not in idle)
        record["waiting_partitions"] = waiting
        record["result"] = LABELS[lang][record["result"]]
        record["fired"] = []
        if watermark is not None:
            for end in sorted(counts):
                if end <= watermark and end not in fired:
                    fired.add(end)
                    record["fired"].append({"window_end": end, "count": counts[end]})
        timeline.append(record)
    return {
        "timeline": timeline,
        "windows": [
            {"end": end, "count": counts[end], "fired": end in fired}
            for end in sorted(counts)
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="FR : expliquer les watermarks / EN: explain watermarks / ES: explicar watermarks"
    )
    parser.add_argument(
        "events", help="JSON Lines: partition and integer ts; optional type=idle/resume"
    )
    parser.add_argument("--window", type=int, default=10)
    parser.add_argument("--out-of-order", type=int, default=2)
    parser.add_argument("--allowed-lateness", type=int, default=0)
    parser.add_argument(
        "--partition",
        action="append",
        help="FR : partition attendue (répéter) / EN: expected partition (repeat) / ES: partición esperada (repetir)",
    )
    parser.add_argument("--lang", choices=LABELS, default="en", help="FR / EN / ES")
    args = parser.parse_args(argv)
    try:
        result = simulate(
            load_events(args.events),
            args.window,
            args.out_of_order,
            args.allowed_lateness,
            partitions=args.partition,
            lang=args.lang,
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
