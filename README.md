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

## Partitions silencieuses / Silent partitions / Particiones silenciosas

FR : Déclarez toutes les partitions attendues avec `--partition A --partition B`. Avant son premier événement, une partition active figure dans `waiting_partitions` et retient le watermark ; `idle` l’exclut, `resume` la réactive. Le watermark ne recule jamais. Sans déclaration, les partitions sont découvertes au fil du replay : une partition jamais vue ne peut pas être devinée. Les paramètres et événements sont validés avant simulation. Le modèle reste pédagogique, sans runtime Flink, alignement ni détection automatique d’inactivité.

EN: Declare every expected partition with `--partition A --partition B`. Before its first event, an active partition appears in `waiting_partitions` and holds back the watermark; `idle` excludes it and `resume` reactivates it. Watermarks never regress. Without declarations, partitions are discovered during replay: an unseen partition cannot be guessed. Parameters and events are validated before simulation. This remains a teaching model, without a Flink runtime, alignment or automatic idle detection.

ES: Declare todas las particiones esperadas con `--partition A --partition B`. Antes de su primer evento, una partición activa aparece en `waiting_partitions` y retiene el watermark; `idle` la excluye y `resume` la reactiva. El watermark nunca retrocede. Sin declaración, las particiones se descubren durante el replay: no se adivinan particiones nunca vistas. Se validan parámetros y eventos antes de simular. Sigue siendo un modelo didáctico, sin runtime Flink, alineación ni detección automática de inactividad.

```sh
python3 watermark_doctor.py examples/cold-partitions.jsonl --partition A --partition B --window 10 --out-of-order 0 --lang fr
python3 watermark_doctor.py examples/cold-partitions.jsonl --partition A --partition B --window 10 --out-of-order 0 --lang en
python3 watermark_doctor.py examples/cold-partitions.jsonl --partition A --partition B --window 10 --out-of-order 0 --lang es
```

FR : le scénario fictif produit les watermarks `null, null, 20, 20, 20, 30`. EN: the fictional scenario produces watermarks `null, null, 20, 20, 20, 30`. ES: el escenario ficticio produce watermarks `null, null, 20, 20, 20, 30`. Source : [Apache Flink — idle sources](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/#dealing-with-idle-sources).


FR : Chaque étape expose `late_updates` : une mise à jour tardive acceptée après le premier déclenchement contient le **compte total de la fenêtre**, pas un incrément. `fired` conserve les premiers déclenchements ; un événement trop tardif n’émet aucune mise à jour. Cela permet de rejouer les sorties du modèle pédagogique sans additionner plusieurs fois les mêmes événements. Ce n’est ni un connecteur de sortie ni un émulateur Flink ; la rétention mémoire des fenêtres reste simplifiée.

EN: Each step exposes `late_updates`: an accepted late update after first firing contains the **full window count**, not an increment. `fired` keeps first firings; a too-late event emits no update. This lets readers replay the teaching model’s outputs without adding the same events twice. It is neither a sink connector nor a Flink emulator; window memory retention remains simplified.

ES: Cada paso incluye `late_updates`: una actualización tardía aceptada tras la primera emisión contiene el **recuento total de la ventana**, no un incremento. `fired` conserva las primeras emisiones; un evento demasiado tardío no emite actualización. Permite reproducir las salidas del modelo didáctico sin sumar varias veces los mismos eventos. No es conector de salida ni emulador Flink; la retención de ventanas sigue simplificada.

## Diagnostic de progression / Progress diagnostics / Diagnóstico del avance

FR — Chaque étape fournit les candidats par partition, les partitions limitantes (y compris les ex æquo), le motif de progression/blocage et son explication FR/EN/ES. Une partition déclarée froide bloque même si un minimum est observable ailleurs ; reprendre une ancienne partition ne recule jamais le watermark. Le modèle pédagogique ne simule pas le runtime Apache Flink.

EN — Each step reports per-partition candidates, limiting partitions including ties, progress/stall reason and a localized explanation. A declared cold partition blocks even when another minimum is observable; resuming an old partition never rewinds the watermark. This teaching model does not emulate the Apache Flink runtime.

ES — Cada paso expone candidatos por partición, particiones limitantes incluidos empates, motivo de avance/bloqueo y explicación localizada. Una partición declarada fría bloquea aunque exista otro mínimo; reanudar una antigua no retrasa el watermark. Este modelo didáctico no emula el runtime Apache Flink.

## Comparaison / Comparison / Comparación

FR — Comparer plusieurs retards admis sur le même ordre d’entrée : --compare-lateness 0 --compare-lateness 5 retourne les étapes rejetées et comptes par fenêtre pour chaque politique.

EN — Compare multiple allowed-lateness values on the same input order: --compare-lateness 0 --compare-lateness 5 returns dropped steps and per-window counts for each policy.

ES — Comparar varios retrasos admitidos en el mismo orden de entrada: --compare-lateness 0 --compare-lateness 5 devuelve pasos descartados y conteos por ventana para cada política.
