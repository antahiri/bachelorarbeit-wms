# AiiDA (Notizen)

Erzeugt mit `verdi node graph generate <PK> -f png -O <name>`. Braucht Graphviz.
Achtung: `-O` haengt die Endung nicht immer an, dann die Datei per `mv`
umbenennen, sonst laesst sie sich nicht oeffnen.

Verwendete PKs: 6147 (Pipeline), 6182 (Scatter-Gather, 4 Chunks), lokale Laeufe
vom 26.07.2026.

Der Graph zeigt die **tatsaechlich ausgefuehrten Instanzen** mit voller
Provenance: bei Scatter-Gather vier getrennte compute-CalcJobs, gespeist aus
derselben FolderData ueber `INPUT_CALC source_folders__split`, wieder
zusammenlaufend im aggregate-CalcJob ueber `source_folders__compute_1` bis
`compute_4`.

Der Preis ist die Dichte: jede Skriptdatei ist ein eigener Knoten, pro CalcJob
kommen zwei Ausgabeknoten dazu (`retrieved`, `remote_folder`). Bei neun Aufgaben
sind das ueber vierzig Knoten. Fuer den Fliesstext zu dicht, als Beleg im Anhang
gut.

Die Variante `-l logic` taugt nicht als schlanke Uebersicht, sie blendet die
CALL_CALC-Kanten aus und damit die CalcJobs selbst. Fuer eine Reduktion eher
`-d 1` (Tiefenbegrenzung).
