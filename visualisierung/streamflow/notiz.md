# StreamFlow (Notizen)

Bietet **keine** Graphausgabe. Stattdessen zwei andere Werkzeuge:

`streamflow report <uuid> --format html [--group-by-step]` erzeugt eine
Zeitleiste der ausgefuehrten Schritte (Plotly, deshalb rund 5 MB je Datei, die
Bibliothek ist eingebettet). Braucht `plotly` im Environment, das StreamFlow
nicht als Pflichtabhaengigkeit mitbringt. Ohne `--group-by-step` erscheinen die
vier compute-Instanzen einzeln, dann ist die Parallelitaet in der Zeitleiste
sichtbar.

`streamflow prov <uuid>` erzeugt ein RO-Crate-Archiv. Inhalt: Manifest
(`ro-crate-metadata.json`), HTML-Vorschau, `workflow.cwl` und die Ein- und
Ausgabedateien inhaltsadressiert ueber SHA1. Also die Inhalte selbst, nicht nur
Verweise.

Wichtiger Befund: Die Erzeugung setzt voraus, dass die Originaldateien noch auf
der Platte liegen. Ein Export fuer einen aelteren Lauf schlug fehl, weil die
`workflow.cwl` unter ihrem damaligen Pfad nicht mehr existierte. Unterschied zu
AiiDA: dort liegen die Inhalte von Anfang an im Repository und ueberleben das
Aufraeumen des Projektverzeichnisses.

Workflow-Namen sind UUIDs, StreamFlow vergibt ohne Konfiguration keine
sprechenden Namen.
