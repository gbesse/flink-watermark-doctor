"""A deterministic teaching model for partition watermarks and tumbling windows.

This is deliberately a small model, not an Apache Flink runtime emulator.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


def load_events(path):
    events = []
    for line_number, raw in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            event = json.loads(raw)
            if not isinstance(event, dict) or not isinstance(event.get("partition"), str):
                raise ValueError("partition must be a string")
            if event.get("type", "event") not in ("event", "idle", "resume"):
                raise ValueError("type must be event, idle or resume")
            if event.get("type", "event") == "event" and (not isinstance(event.get("ts"), int) or isinstance(event.get("ts"), bool)):
                raise ValueError("event ts must be an integer")
        except (json.JSONDecodeError, ValueError) as error:
            raise ValueError(f"{path}:{line_number}: {error}") from error
        events.append(event)
    return events


def simulate(events, window, out_of_order, allowed_lateness):
    if window <= 0 or out_of_order < 0 or allowed_lateness < 0:
        raise ValueError("window must be positive; delays cannot be negative")
    maxima = {}
    idle = set()
    counts = defaultdict(int)
    fired = set()
    watermark = None
    timeline = []
    for number, event in enumerate(events, 1):
        partition = event["partition"]
        kind = event.get("type", "event")
        record = {"step": number, "partition": partition, "type": kind, "watermark_before": watermark}
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
            maxima[partition] = max(timestamp, maxima.get(partition, timestamp))
        active = [max_ts - out_of_order for name, max_ts in maxima.items() if name not in idle]
        if active:
            candidate = min(active)
            watermark = candidate if watermark is None else max(watermark, candidate)
        record["watermark_after"] = watermark
        record["active_partitions"] = sorted(name for name in maxima if name not in idle)
        record["fired"] = []
        if watermark is not None:
            for end in sorted(counts):
                if end <= watermark and end not in fired:
                    fired.add(end)
                    record["fired"].append({"window_end": end, "count": counts[end]})
        timeline.append(record)
    return {"timeline": timeline, "windows": [{"end": end, "count": counts[end], "fired": end in fired} for end in sorted(counts)]}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Explain watermarks with a small deterministic event-time model")
    parser.add_argument("events", help="JSON Lines: partition and integer ts; optional type=idle/resume")
    parser.add_argument("--window", type=int, default=10)
    parser.add_argument("--out-of-order", type=int, default=2)
    parser.add_argument("--allowed-lateness", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        result = simulate(load_events(args.events), args.window, args.out_of_order, args.allowed_lateness)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
