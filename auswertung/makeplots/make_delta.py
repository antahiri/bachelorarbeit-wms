#!/usr/bin/env python3
"""Vergleicht die exklusive und die geteilte Kampagne auf MOGON.
Dargestellt wird die Differenz des Overheads je Konfiguration."""

import csv, statistics
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

BASE = Path.home() / "bachelorarbeit" / "messungen" / "finale_Ergebnisse"
OUT = Path.home() / "bachelorarbeit" / "auswertung"

FARBE = {"streamflow": "#3B82F6", "nextflow": "#F59E0B",
         "merlin": "#10B981", "aiida": "#EF4444"}
NAME = {"streamflow": "StreamFlow", "nextflow": "Nextflow",
        "merlin": "Merlin", "aiida": "AiiDA"}
SYS = ["streamflow", "nextflow", "merlin", "aiida"]

plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": 0.25,
                     "axes.axisbelow": True, "figure.dpi": 150,
                     "axes.spines.top": False, "axes.spines.right": False})

def komma(x, pos=None):
    return f"{x:+g}".replace(".", ",")

def load(camp):
    return {(r["system"], r["pattern"], r["workload"], r["chunks"]): r
            for r in csv.DictReader(open(BASE / camp / "central_results.csv"))}

exc, sha = load("mogon_exc"), load("mogon_sha")

fig, ax = plt.subplots(figsize=(8, 4.5))
for i, s in enumerate(SYS):
    d = [float(exc[k]["overhead_s"]) - float(sha[k]["overhead_s"])
         for k in exc if k[0] == s and k in sha]
    x = [i + (j % 5 - 2) * 0.045 for j in range(len(d))]
    ax.scatter(x, d, color=FARBE[s], alpha=0.55, s=32, zorder=3,
               edgecolors="none")
    m = statistics.median(d)
    ax.plot([i - 0.28, i + 0.28], [m, m], color=FARBE[s], lw=3, zorder=4,
            solid_capstyle="round")
    ax.text(i + 0.33, m, f"{m:+.2f} s".replace(".", ","), fontsize=9,
            va="center", color=FARBE[s], fontweight="bold")

ax.axhline(0, color="black", lw=0.9, zorder=2)
ax.set_xticks(range(len(SYS)))
ax.set_xticklabels([NAME[s] for s in SYS])
ax.set_xlim(-0.6, len(SYS) - 0.2)
ax.set_ylabel("Differenz des Overheads [s]")
ax.yaxis.set_major_formatter(FuncFormatter(komma))
ax.set_title("Exklusiver minus geteilter Knoten\n"
             "je Konfiguration (Punkte) und Median (Balken)", fontsize=11)
fig.tight_layout()
fig.savefig(OUT / "05_delta_exklusiv_geteilt.png")
print("geschrieben:", OUT / "05_delta_exklusiv_geteilt.png")

for s in SYS:
    d = [float(exc[k]["overhead_s"]) - float(sha[k]["overhead_s"])
         for k in exc if k[0] == s and k in sha]
    print(f"  {NAME[s]:11} Median {statistics.median(d):+7.3f} s   "
          f"Spanne {min(d):+.2f} bis {max(d):+.2f} s")
