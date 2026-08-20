import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_central, speichere, komma,
                    NAME, FARBE, WORKLOADS, WORKLOAD_NAME)
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

stil()
zentral = lade_central("lokal_mac")
SYS = ["streamflow", "nextflow", "merlin", "aiida", "pegasus"]
CHUNKS = [1, 2, 4]

# Je Panel eigene Spanne: Pegasus erzwingt lokal grosse Bereiche,
# ein gemeinsamer Faktor wuerde die uebrigen Panels leeren
minmax = {}
for w in WORKLOADS:
    werte = [zentral[(s, "scatter_gather", w, c)]["wms_makespan_s"]
             for s in SYS for c in CHUNKS]
    minmax[w] = (min(werte), max(werte))

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
    ax.set_ylim(minmax[w][0] / 1.25, minmax[w][1] * 1.3)
    ax.yaxis.set_major_locator(LogLocator(subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(FuncFormatter(komma))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_xlabel(f"Teilstücke ({WORKLOAD_NAME[w]}e Rechenlast)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
achsen[0].set_ylabel("Gesamtlaufzeit in s (log$_{10}$)")
achsen[1].legend(ncol=len(SYS), loc="upper center",
                 bbox_to_anchor=(0.5, -0.28), frameon=False)

speichere(fig, "lokal", "03_makespan_scatter_gather.png")
