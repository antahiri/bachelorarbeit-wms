"""Gesamtlaufzeit des Scatter-Gather-Musters ueber die Teilstueckzahl,
MOGON im exklusiven Betrieb. Je Rechenlast ein Panel, logarithmische
Achse. Alle drei Panels ueberdecken denselben Faktor auf der y-Achse,
damit die Steigungen vergleichbar sind. Oben und rechts offen, damit
sich der weitere Verlauf gedanklich fortsetzen laesst.

Aufruf aus dem Repo-Wurzelverzeichnis:
    python3 auswertung/plots_final/mogon/03_makespan_scatter_gather.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_central, speichere, komma,
                    NAME, FARBE, WORKLOADS, WORKLOAD_NAME)
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

stil()
zentral = lade_central("mogon_exc")
SYS = ["streamflow", "nextflow", "merlin", "aiida"]
CHUNKS = [1, 2, 4]

# Gemeinsamer Faktor der y-Spannen, damit log-Steigungen vergleichbar sind
minmax = {}
faktor = 0
for w in WORKLOADS:
    werte = [zentral[(s, "scatter_gather", w, c)]["wms_makespan_s"]
             for s in SYS for c in CHUNKS]
    minmax[w] = (min(werte), max(werte))
    faktor = max(faktor, (max(werte) * 1.15) / (min(werte) / 1.15))

fig, achsen = plt.subplots(1, 3, figsize=(11.0, 3.6))
for ax, w in zip(achsen, WORKLOADS):
    for s in SYS:
        werte = [zentral[(s, "scatter_gather", w, c)]["wms_makespan_s"]
                 for c in CHUNKS]
        ax.plot(CHUNKS, werte, marker="o", markersize=5, lw=1.6,
                color=FARBE[s], label=NAME[s])
    ax.set_xscale("log", base=2)
    ax.set_xticks(CHUNKS)
    ax.set_xticklabels([str(c) for c in CHUNKS])
    ax.set_yscale("log")
    unten = minmax[w][0] / 1.15
    ax.set_ylim(unten, unten * faktor)
    ax.yaxis.set_major_formatter(FuncFormatter(komma))
    ax.yaxis.set_minor_formatter(FuncFormatter(komma))
    ax.tick_params(axis="y", which="minor", labelsize=7)
    ax.set_xlabel(f"Teilstücke ({WORKLOAD_NAME[w]}e Rechenlast)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
achsen[0].set_ylabel("Gesamtlaufzeit in s (log$_{10}$)")
achsen[1].legend(ncol=len(SYS), loc="upper center",
                 bbox_to_anchor=(0.5, -0.28), frameon=False)

speichere(fig, "mogon", "03_makespan_scatter_gather_mogon.png")
