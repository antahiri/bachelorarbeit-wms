# Workflow-Erstellung

Die Workflow-Beschreibungen und Konfigurationsdateien je System und Muster.

**Aufbau:** Direkt im Musterordner liegt die Fassung für die lokale Ausführung
und die dedizierte Messung auf MOGON. Der Ordner `konfiguration_fuer_mogon/`
enthält die Fassung für den Slurm-Modus.

**Varianten:** Die Unterordner `short/`, `medium/`, `long/` (Pipeline) und
`<rechenlast>_c<n>/` (Scatter-Gather) enthalten je eine Fassung pro Kombination.


Die Rechenskripte sind für alle Systeme identisch und liegen unter
`../benchmark_scripts/`.