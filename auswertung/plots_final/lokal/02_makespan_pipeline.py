"""Gesamtlaufzeit des Pipeline-Musters je Rechenlast, lokale Ebene. Gruppierte Balken je Rechenlast, Hoehe ist der
Median der Wiederholungen aus v2.

Aufruf aus dem Repo-Wurzelverzeichnis:
    python3 auswertung/plots_final/lokal/02_makespan_pipeline.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_central, speichere, dezimal, komma,
                    NAME, FARBE, WORKLOADS, WORKLOAD_NAME, GROESSE_VOLL)
import matplotlib.pyplot as plt

stil()
zentral = lade_central("lokal_mac")
SYS = ["streamflow", "nextflow", "merlin", "aiida", "pegasus"]

fig, ax = plt.subplots(figsize=GROESSE_VOLL)
breite = 0.16
for j, s in enumerate(SYS):
    werte = [zentral[(s, "pipeline", w, 1)]["wms_makespan_s"] for w in WORKLOADS]
    x = [i + (j - (len(SYS) - 1) / 2) * breite for i in range(len(WORKLOADS))]
    ax.bar(x, werte, width=breite, color=FARBE[s], label=NAME[s])
    for xi, v in zip(x, werte):
        text = komma(round(v, 1)) if v < 100 else komma(round(v))
        ax.annotate(text, (xi, v), textcoords="offset points",
                    xytext=(0, 2.5), ha="center", fontsize=7.5)

ax.set_xticks(range(len(WORKLOADS)))
ax.set_xticklabels([WORKLOAD_NAME[w] for w in WORKLOADS])
ax.set_xlabel("Rechenlast")
ax.set_yscale("log")
ax.set_ylabel("Gesamtlaufzeit in s (log$_{10}$)")
ax.margins(y=0.18)
ax.legend(ncol=len(SYS), loc="upper left", frameon=True)
dezimal(ax)

speichere(fig, "lokal", "02_makespan_pipeline.png")
