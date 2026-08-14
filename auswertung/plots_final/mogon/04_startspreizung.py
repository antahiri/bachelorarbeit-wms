"""Startspreizung der parallelen Verarbeitungsinstanzen, MOGON im
exklusiven Betrieb. Je System zwei Balken (2 und 4 Teilstuecke),
Hoehe ist der Median ueber die drei Rechenlasten. Logarithmische
Achse, Beschriftung in Millisekunden.

Aufruf aus dem Repo-Wurzelverzeichnis:
    python3 auswertung/plots_final/mogon/04_startspreizung.py
"""
import sys
import statistics
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_koordination, speichere, komma,
                    NAME, FARBE, WORKLOADS, GROESSE_VOLL)
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
from matplotlib.ticker import FuncFormatter

stil()
koord = lade_koordination("mogon_exc")
SYS = ["streamflow", "nextflow", "merlin", "aiida"]
CHUNKS = [2, 4]

fig, ax = plt.subplots(figsize=GROESSE_VOLL)
breite = 0.34
for i, s in enumerate(SYS):
    for j, c in enumerate(CHUNKS):
        werte = [koord[(s, "scatter_gather", w, c)]["wms_start_spread_s"]
                 for w in WORKLOADS]
        med = statistics.median(werte)
        x = i + (j - 0.5) * breite
        farbe = to_rgba(FARBE[s], 0.45) if c == 2 else FARBE[s]
        ax.bar(x, med, width=breite, color=farbe)
        ax.annotate(f"{med*1000:.0f} ms", (x, med),
                    textcoords="offset points", xytext=(0, 2.5),
                    ha="center", fontsize=8)

ax.set_yscale("log")
ax.yaxis.set_major_formatter(FuncFormatter(komma))
ax.set_xticks(range(len(SYS)))
ax.set_xticklabels([NAME[s] for s in SYS])
ax.set_ylabel("Startspreizung in s (log$_{10}$)")
ax.margins(y=0.15)
handles = [plt.Rectangle((0, 0), 1, 1, color=to_rgba("#4B5563", 0.45)),
           plt.Rectangle((0, 0), 1, 1, color="#4B5563")]
ax.legend(handles, ["2 Teilstücke", "4 Teilstücke"],
          loc="upper left", frameon=True)

speichere(fig, "mogon", "04_startspreizung_mogon.png")
