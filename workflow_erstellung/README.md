# Workflow-Erstellung

Je System und Muster gibt es zwei Ebenen:

- **Direkt im Musterordner**: die Fassung fuer die lokale Ausfuehrung auf dem
  MacBook und fuer die dedizierte Messung auf MOGON. In beiden Faellen laufen
  die Aufgaben direkt auf dem Rechner beziehungsweise auf dem zugeteilten Knoten.
- **`konfiguration_fuer_mogon/`**: die Fassung fuer den Slurm-Modus auf MOGON,
  in dem jede Aufgabe als eigener Job eingereicht wird mit den Angaben, die
  der Scheduler benoetigt.

Bei **Merlin** enthaelt `konfiguration_fuer_mogon/` stattdessen den Betrieb innerhalb einer Allocation, da Merlin keine Jobs pro Prozess einreicht.

**Pegasus** hat keinen MOGON-Unterordner, da HTCondor auf dem Cluster nicht
verfuegbar war (siehe `pegasus/pegasus_mogon_befund.txt`).

Die Unterordner `short/`, `medium/`, `long/` (Pipeline) und `<workload>_c<n>/`
(Scatter-Gather mit 1, 2 oder 4 Chunks) enthalten je Kombination eine einzelne Fassung. Bei AiiDA gibt es keine Variantenordner, da Chunk-Zahl und
Workload dem Submit-Skript als Argumente uebergeben werden.

Die Benchmark-Skripte sind fuer alle Systeme identisch und liegen unter
`../benchmark_scripts/`.
