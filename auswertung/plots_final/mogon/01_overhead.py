import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_central, speichere, dezimal,
                    NAME, FARBE, WORKLOADS, WORKLOAD_NAME)
import matplotlib.pyplot as plt

stil()
zentral = lade_central("mogon_exc")
SYS = ["streamflow", "nextflow", "merlin", "aiida"]

# Reihenfolge: Pipeline (nur c1), dann Scatter-Gather mit 1, 2, 4
konfigs = [("pipeline", w, 1) for w in WORKLOADS]
konfigs += [("scatter_gather", w, c) for w in WORKLOADS for c in (1, 2, 4)]

def beschriftung(k):
    muster = "P" if k[0] == "pipeline" else "SG"
    return f"{muster}\n{WORKLOAD_NAME[k[1]]}\nc{k[2]}"

fig, ax = plt.subplots(figsize=(10.5, 4.2))
breite = 0.2
for j, s in enumerate(SYS):
    werte = [zentral[(s, *k)]["overhead_s"] for k in konfigs]
    x = [i + (j - (len(SYS) - 1) / 2) * breite for i in range(len(konfigs))]
    ax.bar(x, werte, width=breite, color=FARBE[s], label=NAME[s])

ax.set_xticks(range(len(konfigs)))
ax.set_xticklabels([beschriftung(k) for k in konfigs])
ax.set_ylabel("Overhead in s")
ax.legend(ncol=len(SYS), loc="upper left", frameon=True)
dezimal(ax)

speichere(fig, "mogon", "01_overhead_alle_mogon.png")
