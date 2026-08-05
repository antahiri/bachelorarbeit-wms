# Pegasus (Notizen)

Wird **ohne Zutun** bei jedem Lauf erzeugt und liegt im Submit-Verzeichnis unter
`submit/<user>/pegasus/<workflow>/run0001/`. Drei Dateien je Lauf:

- `*-abstract.png`: der beschriebene Workflow, sechs Schritte
- `*-abstract-files.png`: derselbe mit Datendateien als eigenen Knoten
  (chunk_1.txt bis chunk_4.txt, result_1.txt bis result_4.txt)
- `*-concrete.png`: der **geplante** Workflow nach der Planungsstufe

Der concrete-Graph ist der interessanteste und in keinem anderen System
vorhanden. Aus neun beschriebenen Aufgaben werden dreizehn geplante Jobs.
Ergaenzt hat Pegasus: `create_dir` (Arbeitsverzeichnis im Pool anlegen),
`stage_out_remote` (Ergebnisse zurueckholen), `register` (im Katalog eintragen),
`cleanup` (Arbeitsverzeichnis abraeumen).

Zwei Punkte fuer die Arbeit: Das belegt die Trennung von Beschreibung und
Planung, und die Jobnamen tragen `condorpool`, also die Zielumgebung. Der
geplante Workflow ist damit fest auf HTCondor bezogen, was den MOGON-Befund
erklaert. Die vier Zusatzjobs erklaeren ausserdem einen Teil des hohen lokalen
Overheads.
