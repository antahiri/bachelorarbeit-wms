import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_central, speichere, dezimal, komma,
                    NAME, FARBE, WORKLOADS, WORKLOAD_NAME, GROESSE_VOLL)
import matplotlib.pyplot as plt

stil()
zentral = lade_central("mogon_exc")
SYS = ["streamflow", "nextflow", "merlin", "aiida"]

fig, ax = plt.subplots(figsize=GROESSE_VOLL)
breite = 0.2
for j, s in enumerate(SYS):
    werte = [zentral[(s, "pipeline", w, 1)]["wms_makespan_s"] for w in WORKLOADS]
    x = [i + (j - (len(SYS) - 1) / 2) * breite for i in range(len(WORKLOADS))]
    ax.bar(x, werte, width=breite, color=FARBE[s], label=NAME[s])
    for xi, v in zip(x, werte):
        ax.annotate(komma(round(v, 1)), (xi, v), textcoords="offset points",
                    xytext=(0, 2.5), ha="center", fontsize=7.5)

ax.set_xticks(range(len(WORKLOADS)))
ax.set_xticklabels([WORKLOAD_NAME[w] for w in WORKLOADS])
ax.set_xlabel("Rechenlast")
ax.set_ylabel("Gesamtlaufzeit in s")
ax.set_ylim(0, max(zentral[(s, "pipeline", "long", 1)]["wms_makespan_s"]
                   for s in SYS) * 1.12)
ax.legend(ncol=len(SYS), loc="upper left", frameon=True, fontsize=9)
dezimal(ax)

speichere(fig, "mogon", "02_makespan_pipeline_mogon.png")
