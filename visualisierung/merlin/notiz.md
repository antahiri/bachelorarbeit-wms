# Merlin (Notizen)

Bietet **keine** Visualisierung des Workflow-Graphen. `merlin --help` kennt
weder graph noch dag noch visual.

Intern existiert ein DAG durchaus (Klasse `merlin.study.dag.DAG`, aufgebaut aus
Maestros ExecutionGraph), er dient aber ausschliesslich der Ausfuehrungsplanung:
`group_tasks` und `find_independent_chains` zerlegen ihn in unabhaengige Ketten,
die als Celery-Gruppen abgeschickt werden. Eine Ausgabe gibt es nicht.
Formulierung fuer die Arbeit deshalb: "stellt keine Visualisierung bereit",
nicht "hat keinen DAG".

Was Merlin stattdessen ablegt: das Verzeichnis `merlin_info/` je Lauf mit drei
Fassungen der Spezifikation, `orig` (wie eingereicht), `partial` und `expanded`
(nach Aufloesung von Variablen und Parametern). Das ist Merlins Beitrag zur
Nachvollziehbarkeit und hat bei der Klaerung der Worker-Konfiguration
tatsaechlich geholfen.

Hier abgelegt: die drei Fassungen aus dem lokalen Lauf vom 26.07.2026.
