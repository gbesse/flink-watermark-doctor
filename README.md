# Flink Watermark Doctor

A small, replayable model that explains how event timestamps, partition watermarks and idle partitions affect tumbling windows. It is a learning and design aid, **not an Apache Flink runtime emulator**.

```sh
python3 watermark_doctor.py examples/two-partitions.jsonl --window 10 --out-of-order 2
python3 -m unittest discover -s tests
```

Each JSON line has `partition` and integer `ts`, or `partition` plus `type: "idle"` / `"resume"`. Units are arbitrary but must be consistent. The model sets each active partition watermark to its maximum seen timestamp minus `--out-of-order`, then takes the minimum across active partitions. Watermarks never move backward. Windows are half-open `[start,end)`; an event is discarded when the current watermark has reached `end + --allowed-lateness`. The output shows every step, accepted or late events, and window firings. It does not model Flink's full trigger, checkpoint, source-idleness timeout or state-cleanup semantics. See [Flink's event-time debugging guide](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/debugging/debugging_event_time/).

## Français

Rejouez des événements horodatés pour comprendre pourquoi une fenêtre attend ou rejette un événement tardif. Le fichier JSON Lines contient `partition` et `ts`, ou un événement `idle` / `resume`. Les paramètres et limites du modèle sont décrits ci-dessus. Ce simulateur n’exécute pas Flink.

## Español

Reproduzca eventos con marcas de tiempo para entender por qué una ventana espera o descarta un evento tardío. El archivo JSON Lines contiene `partition` y `ts`, o un evento `idle` / `resume`. Los parámetros y límites del modelo se describen arriba. Este simulador no ejecuta Flink.

MIT licensed.
