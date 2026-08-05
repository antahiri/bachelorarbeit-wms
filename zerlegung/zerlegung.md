# Zeitzerlegung im Slurm-Modus

Dieser Ordner enthaelt die Artefakte, aus denen die Zerlegung der Zeit zwischen
dem Einreichen eines Jobs und dem ersten Zeitstempel im Payload abgeleitet wurde.
Je System liegen das erzeugte Jobskript, die Zeitstempel des Arbeitsverzeichnisses,
die Ausgabe von `sacct` und die Zeitmessung des Payloads vor.

Alle Zeiten stammen aus den beigelegten Dateien und lassen sich daraus
nachrechnen. Die Zeitstempel der Dateien wurden mit
`ls -la --time-style=full-iso` erfasst, die Payload-Zeiten stehen als
Nanosekundenwerte in den `timing_*.txt`.

---

## Uebersicht

| System | Job | Schritt | Block bis zur Marke | Marke bis Payload | Payload |
|---|---|---|---|---|---|
| Nextflow | 460503 | preprocess | rund 44 s | 55,0 ms | 4,27 ms |
| StreamFlow | 463674 | generate_input | 44,5 s | 102,1 ms | 1,86 ms |
| AiiDA | 461447 | preprocess | 54,3 s | 103,8 ms | 2,68 ms |
| Merlin | 455287 | alle neun Schritte | 43,0 s, einmalig | siehe unten | neun Schritte in 1,76 s |

Der Block umfasst im Wesentlichen den Prolog, den Slurm zur Laufzeit des Jobs
zaehlt. Der mittlere Wert ist der Anteil, den das jeweilige System innerhalb des
Jobs benoetigt, bis der Payload seinen ersten eigenen Zeitstempel setzt.

---

## Nextflow, Job 460503

Dateien: `command_run_460503.sh`, `command_sh_460503.sh`,
`dateizeiten_460503.txt`, `sacct_460503.txt`, `timing_preprocess.txt`

Nextflow erzeugt je Aufgabe ein eigenes Arbeitsverzeichnis und schreibt zwei
Skripte hinein. `.command.run` ist der Wrapper, den Slurm startet, und besteht
groesstenteils aus Funktionsdefinitionen. Ausgefuehrt werden davon nur die
Optionen am Anfang und der Aufruf der Hauptfunktion am Ende. Diese setzt zuerst
die Signalbehandlung, wechselt in das Arbeitsverzeichnis und legt dann die
Markerdatei `.command.begin` an. `.command.sh` enthaelt den eigentlichen Aufruf
des Payloads.

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

Dateien: `_aiidasubmit.sh`, `dateizeiten_461447.txt`, `sacct_461447.txt`,
`timing_preprocess.txt`

Der Daemon legt vor dem Einreichen das Arbeitsverzeichnis an, kopiert die
benoetigten Skripte hinein und schreibt das Jobskript. Zwischen dem Schreiben
des Jobskripts (09:43:31,685305) und der letzten kopierten Datei
(09:43:31,908299) liegen 223 ms. Diese Vorarbeit faellt vor dem Job an und
erscheint in keiner Slurm-Statistik.

Das erzeugte Jobskript umfasst 440 Byte. Nach dem SBATCH-Kopf enthaelt es genau
eine Befehlszeile, die den Payload direkt aufruft und dessen Ausgaben in zwei
Dateien umleitet.

Marken: `_scheduler-stdout.txt`, 09:44:26,264486, wird von slurmd angelegt, bevor
eine Shell startet, und ist damit die frueheste verfuegbare Marke.
`aiida_stdout.txt`, 09:44:26,279408, entsteht durch die Umleitung in der
Befehlszeile, also sobald die Shell diese Zeile abarbeitet. Payload-Start:
09:44:26,368334.

Daraus ergeben sich 14,9 ms fuer die Shell und 88,9 ms fuer den Start des
Python-Interpreters, zusammen 103,8 ms. Diese Aufteilung ist nur bei AiiDA
moeglich, da die frueheste Marke hier vor der Shell entsteht.

Nach dem Ende des Jobs holt der Daemon die Ergebnisse zurueck und wertet sie
aus. Auch dieser Anteil erscheint in keiner Slurm-Statistik.

---

## Merlin, Job 455287

Dateien: `worker_job.sbatch`, `compute_1.sh`, `worker_marken_455287.txt`,
`sacct_455287.txt`

Merlin reicht keine Jobs je Aufgabe ein. Die Aufgaben werden ueber einen
Nachrichtenbroker an Arbeitsprozesse verteilt, die innerhalb einer bestehenden
Allocation laufen. `worker_job.sbatch` ist die einzige Slurm-Einreichung des
gesamten Laufs und wurde nicht erzeugt, sondern selbst geschrieben.

Entsprechend enthaelt das von Merlin erzeugte Schrittskript `compute_1.sh`
keinen SBATCH-Kopf, sondern nur das Kopieren der Eingabe und den Aufruf des
Payloads.

Zeitpunkte laut `sacct_455287.txt` und `worker_marken_455287.txt`:

- Start der Allocation: 20:38:54
- Verbindung zum Broker: 20:39:37,012, also 43,0 s fuer Prolog, Laden der
  Module, Aktivieren der Umgebung und Start der Arbeitsprozesse
- Bereitschaft: 20:39:38,043, also 1,03 s weiter
- erster Schritt: 20:39:43,703
- letzter Schritt: 20:39:45,463, alle neun Schritte in 1,76 s

Zur Einordnung der 5,66 s zwischen Bereitschaft und erstem Schritt: In diesem
Lauf wurden die Arbeitsprozesse vor dem Einreihen der Aufgaben gestartet. Der
Zeitstempel im Namen des Studienverzeichnisses (20:39:43) zeigt, dass die
Aufgaben erst zu diesem Zeitpunkt erzeugt wurden. Es handelt sich also um
Wartezeit auf Arbeit, nicht um Bearbeitungszeit. In einem Lauf mit umgekehrter
Reihenfolge lag zwischen Bereitschaft und erstem Schritt 160 ms.

Die Allocation lief nach Abschluss der Arbeit bis zum Zeitlimit weiter
(Elapsed 15:21), da die Arbeitsprozesse auf weitere Aufgaben warten.

---

## Ergebnis

Der Anteil, den die Systeme innerhalb eines Jobs benoetigen, liegt zwischen 55
und 104 Millisekunden. Der Anteil, der auf Warteschlange und Prolog entfaellt,
liegt zwischen 43 und 54 Sekunden und schwankt zwischen einzelnen Jobs deutlich.
Bei Merlin faellt dieser Anteil einmal je Allocation an statt einmal je Aufgabe.

Die Systeme unterscheiden sich zudem darin, wo sie ihre Arbeit verrichten.
Nextflow legt sie in den Job selbst, StreamFlow erledigt sie vorher im Treiber,
AiiDA vorher und nachher im Daemon, und Merlin verlagert sie in dauerhaft
laufende Arbeitsprozesse.
