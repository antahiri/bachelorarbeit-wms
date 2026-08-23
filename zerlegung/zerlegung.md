# Zeitzerlegung im Slurm-Modus

Dieser Ordner enthaelt die Artefakte, aus denen die Zerlegung der Zeit zwischen
dem Einreichen eines Jobs und dem ersten Zeitstempel im Payload abgeleitet wurde.

Die Untersuchung ist systemspezifisch, weil sich Aufbau und Ablauf der
Jobskripte zwischen den Systemen unterscheiden. Bei Nextflow und StreamFlow
laesst sich ein Zeitraum zwischen einer Marke im Job und dem Payload angeben,
bei AiiDA und Merlin ist die Aussage struktureller Art.

Die Zeitstempel der Dateien wurden mit `ls -la --time-style=full-iso` erfasst,
die Payload-Zeiten stehen als Nanosekundenwerte in den `timing_*.txt`.

---

## Uebersicht

| System | Job | Marke bis Payload | Bemerkung |
|---|---|---|---|
| Nextflow | 460503 | 55,0 ms | Wrapper mit Staging vor dem Payload |
| StreamFlow | 463674 | 102,1 ms | Verzeichniswechsel und zwei Umgebungsvariablen |
| AiiDA | 461447 | nicht bestimmt | nach dem SBATCH-Kopf unmittelbar der Payload-Aufruf |
| Merlin | 455287 | entfaellt | keine Einreichung je Aufgabe |

Der Block bis zur Marke, also im Wesentlichen Warteschlange und Prolog, lag in
den betrachteten Jobs bei 43 bis 54 Sekunden und schwankte zwischen einzelnen
Jobs deutlich.

Wichtig: Die angegebenen Zeitraeume sind keine reinen Systemanteile. Sie
enthalten auch den Start von Shell und Python-Interpreter und sind damit eine
Obergrenze dessen, was das jeweilige System innerhalb des Jobs vor dem Payload
tut.

---

## Nextflow, Job 460503

Dateien: `command_run_460503.sh`, `command_sh_460503.sh`,
`dateizeiten_460503.txt`, `sacct_460503.txt`, `timing_preprocess.txt`

Nextflow erzeugt je Aufgabe ein eigenes Arbeitsverzeichnis und schreibt zwei
Skripte hinein. `.command.run` ist der Wrapper, den Slurm startet, und besteht
groesstenteils aus Funktionsdefinitionen. Ausgefuehrt werden davon nur die
Optionen am Anfang und der Aufruf der Hauptfunktion am Ende. Diese setzt zuerst
die Signalbehandlung, wechselt in das Arbeitsverzeichnis und legt dann die
Markerdatei `.command.begin` an. Anschliessend folgt das Staging der
Eingabedateien und der Aufruf von `.command.sh`, das den Payload startet.

Marke: `.command.begin`, 13:10:46,248565. Payload-Start laut
`timing_preprocess.txt`: 13:10:46,303560. Differenz 55,0 ms.

Einschraenkung: Die Marke wird erst nach der Signalbehandlung und dem
Verzeichniswechsel gesetzt und liegt damit einige Millisekunden hinter dem
tatsaechlichen Ende des Prologs. Der ausgewiesene Anteil ist deshalb eine
Untergrenze. Die von Slurm angelegte Ausgabedatei `.command.log` waere frueher,
ist aber unbrauchbar, da der Wrapper spaeter hineinschreibt und ihr Zeitstempel
dadurch wandert.

Der Wrapper umfasst in diesem Lauf 3688 Byte. Bei aktivierter
Trace-Instrumentierung waechst er auf 9968 Byte, da sich der Wrapper dann selbst
ein zweites Mal aufruft.

---

## StreamFlow, Job 463674

Dateien: `slurm-463674.sh`, `dateizeiten_463674.txt`, `sacct_463674.txt`,
`timing_generate_input.txt`

StreamFlow uebergibt das Jobskript ueber die Standardeingabe an `sbatch`. Es
existiert daher keine Skriptdatei im Arbeitsverzeichnis. Die beigelegte Datei
`slurm-463674.sh` wurde waehrend der Laufzeit mit
`scontrol write batch_script 463674` aus dem Slurm-Controller ausgelesen.

Das Skript besteht aus zwei Zeilen und enthaelt keinen SBATCH-Kopf, da alle
Ressourcenangaben als Kommandozeilenoptionen an `sbatch` uebergeben werden.
Deshalb traegt der Job auch keinen eigenen Namen, sondern den Standardnamen
`sbatch`. Die Befehlszeile wechselt in das Arbeitsverzeichnis, setzt zwei
Umgebungsvariablen und ruft den Payload auf.

Marke: `slurm-463674.out`, 18:17:55,539525. Diese Datei wird von slurmd beim
Start des Jobs angelegt und bleibt leer, da der Payload nichts ausgibt. Ihr
Zeitstempel bleibt dadurch unveraendert und markiert den Startzeitpunkt.
Payload-Start: 18:17:55,641665. Differenz 102,1 ms.

Einschraenkung: Das Verfahren funktioniert nur, solange der Payload keine
Ausgaben erzeugt. Bei sprechender Ausgabe wandert der Zeitstempel mit.

Ein zweiter Lauf desselben Workflows (Job 463679, preprocess) ergab an derselben
Stelle 85,7 ms. Der Millisekundenanteil ist damit als Groessenordnung zu lesen,
nicht als exakter Wert.

---

## AiiDA, Job 461447

Datei: `_aiidasubmit.sh`

Der Daemon legt das Arbeitsverzeichnis an, kopiert die benoetigten Skripte
hinein und schreibt das Jobskript, bevor der Job eingereicht wird. Nach dem
Ende des Jobs holt der Daemon die Ergebnisse zurueck und wertet sie aus. Beide
Anteile liegen ausserhalb des Slurm-Jobs und erscheinen in keiner
Slurm-Statistik.

Das erzeugte Jobskript umfasst 440 Byte. Nach dem SBATCH-Kopf enthaelt es genau
eine Befehlszeile, die den Payload aufruft und dessen Ausgaben in zwei Dateien
umleitet. Innerhalb des Jobs finden vor dem Payload also keine weiteren
Vorbereitungsschritte statt.

Ein Zeitraum wird hier bewusst nicht angegeben. Er bestuende praktisch
vollstaendig aus dem Start von Shell und Python-Interpreter und waere damit
kein Anteil des Systems.

---

## Merlin, Job 455287

Datei: `worker_job.sbatch`

Merlin reicht keine Jobs je Aufgabe ein. Die Aufgaben werden ueber einen
Nachrichtenbroker an Arbeitsprozesse verteilt, die innerhalb einer bestehenden
Allocation laufen. `worker_job.sbatch` ist die einzige Slurm-Einreichung des
gesamten Laufs und wurde nicht erzeugt, sondern selbst geschrieben.

Die Betrachtung je Workflow-Schritt greift hier deshalb nicht. Warteschlange
und Prolog fallen einmal je Allocation an statt einmal je Aufgabe.

Beobachtung aus dem Lauf, nicht Teil der Arbeit: Zwischen dem Start der
Allocation und der Bereitschaft der Arbeitsprozesse lagen rund 44 Sekunden,
danach liefen alle neun Schritte des Scatter-Gather-Musters innerhalb von
1,76 Sekunden. Die Allocation lief nach Abschluss der Arbeit bis zum Zeitlimit
weiter, da die Arbeitsprozesse auf weitere Aufgaben warten.

---

## Ergebnis

Der Zeitraum zwischen einer Marke im Job und dem Payload betraegt bei Nextflow
55 ms und bei StreamFlow 102 ms, jeweils einschliesslich Prozessstart. Bei
AiiDA folgt nach dem SBATCH-Kopf unmittelbar der Payload-Aufruf. Dem steht ein
Anteil von 43 bis 54 Sekunden fuer Warteschlange und Prolog gegenueber, der
zwischen einzelnen Jobs deutlich schwankt.

Die Systeme unterscheiden sich zudem darin, wo sie ihre Arbeit verrichten.
Nextflow legt sie in den Job selbst, StreamFlow erledigt sie groesstenteils
vorher im Treiber, AiiDA vorher und nachher im Daemon, und Merlin verlagert sie
in dauerhaft laufende Arbeitsprozesse. Ein einheitlich definierter Anteil je
System laesst sich daraus nicht bestimmen.