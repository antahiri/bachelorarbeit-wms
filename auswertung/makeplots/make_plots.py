#!/usr/bin/env python3
"""Erzeugt die Auswertungsgrafiken einer Messkampagne.
Aufruf: python3 make_plots.py {lokal_mac|mogon_exc|mogon_sha}"""

import csv, statistics, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch

KAMPAGNE = sys.argv[1] if len(sys.argv) > 1 else "lokal_mac"
TITEL = {"lokal_mac": "lokal, MacBook", "mogon_exc": "MOGON, exklusiv",
         "mogon_sha": "MOGON, geteilt"}[KAMPAGNE]

BASE = Path.home() / "bachelorarbeit" / "messungen" / "finale_Ergebnisse" / KAMPAGNE
OUT = Path.home() / "bachelorarbeit" / "auswertung" / KAMPAGNE
OUT.mkdir(parents=True, exist_ok=True)

FARBE = {"streamflow": "#3B82F6", "nextflow": "#F59E0B", "merlin": "#10B981",
         "aiida": "#EF4444", "pegasus": "#8B5CF6"}
NAME = {"streamflow": "StreamFlow", "nextflow": "Nextflow", "merlin": "Merlin",
        "aiida": "AiiDA", "pegasus": "Pegasus"}
WL = ["short", "medium", "long"]

plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": 0.25,
                     "axes.axisbelow": True, "figure.dpi": 150,
                     "axes.spines.top": False, "axes.spines.right": False})

def komma(x, pos=None):
    return f"{x:g}".replace(".", ",")
FMT = FuncFormatter(komma)

def load(kind):
    return {(r["system"], r["pattern"], r["workload"], r["chunks"]): r
            for r in csv.DictReader(open(BASE / f"{kind}_results.csv"))}

cen, coo = load("central"), load("coordination")
SYS = [s for s in ["streamflow", "nextflow", "merlin", "aiida", "pegasus"]
       if any(k[0] == s for k in cen)]
logy = "pegasus" in SYS  # Pegasus sprengt lineare Achsen

# --- 1. Overhead, beide Muster, alle Konfigurationen ----------------------
fig, ax = plt.subplots(figsize=(10, 4))
labels, xpos, bw = [], 0, 0.8 / len(SYS)
for pat, chunks in [("pipeline", ["1"]), ("scatter_gather", ["1", "2", "4"])]:
    for w in WL:
        for c in chunks:
            for i, s in enumerate(SYS):
                v = float(cen[(s, pat, w, c)]["overhead_s"])
                ax.bar(xpos + i * bw, v, bw, color=FARBE[s])
            labels.append((xpos + 0.4 - bw / 2,
                           f"{'P' if pat == 'pipeline' else 'SG'}\n{w[:3]}\nc{c}"))
            xpos += 1.1
if logy:
    ax.set_yscale("log")
ax.set_xticks([p for p, _ in labels])
ax.set_xticklabels([l for _, l in labels], fontsize=7)
ax.set_ylabel("Overhead [s]" + (", logarithmisch" if logy else ""))
ax.yaxis.set_major_formatter(FMT)
ax.set_title(f"Overhead gegenüber der Referenz, alle Konfigurationen ({TITEL})")
ax.legend(handles=[Patch(facecolor=FARBE[s], label=NAME[s]) for s in SYS],
          ncol=len(SYS), fontsize=8)
fig.tight_layout(); fig.savefig(OUT / "01_overhead_alle.png"); plt.close(fig)

# --- 2. Makespan Pipeline -------------------------------------------------
fig, ax = plt.subplots(figsize=(7.5, 4))
bw = 0.8 / len(SYS)
for i, s in enumerate(SYS):
    werte = [float(cen[(s, "pipeline", w, "1")]["wms_makespan_s"]) for w in WL]
    x = [j + (i - len(SYS) / 2 + 0.5) * bw for j in range(3)]
    ax.bar(x, werte, bw, label=NAME[s], color=FARBE[s])
    for xi, v in zip(x, werte):
        lbl = f"{v:.1f}".replace(".", ",") if v < 100 else f"{v:.0f}"
        ax.text(xi, v * (1.1 if logy else 1.01), lbl, ha="center", fontsize=6.5)
if logy:
    ax.set_yscale("log")
ax.set_xticks(range(3)); ax.set_xticklabels(WL)
ax.set_ylabel("Makespan [s]" + (", logarithmisch" if logy else ""))
ax.set_xlabel("Workload")
ax.yaxis.set_major_formatter(FMT)
ax.set_title(f"Pipeline: Makespan ({TITEL})")
ax.legend(ncol=len(SYS), fontsize=8, loc="upper left")
fig.tight_layout(); fig.savefig(OUT / "02_makespan_pipeline.png"); plt.close(fig)

# --- 3. Makespan Scatter-Gather über Chunks -------------------------------
fig, axes = plt.subplots(1, 3, figsize=(11, 3.6))
for axi, w in zip(axes, WL):
    for s in SYS:
        y = [float(cen[(s, "scatter_gather", w, c)]["wms_makespan_s"])
             for c in ["1", "2", "4"]]
        axi.plot([1, 2, 4], y, "o-", color=FARBE[s], label=NAME[s], lw=1.8, ms=5)
    axi.set_xscale("log", base=2)
    axi.set_xticks([1, 2, 4]); axi.set_xticklabels(["1", "2", "4"])
    axi.set_yscale("log")
    axi.set_title(w); axi.set_xlabel("Chunks")
    axi.yaxis.set_major_formatter(FMT)
axes[0].set_ylabel("Makespan [s]")
handles, labels_ = axes[0].get_legend_handles_labels()
fig.legend(handles, labels_, ncol=len(SYS), fontsize=8,
           loc="lower center", bbox_to_anchor=(0.5, -0.08), frameon=False)
fig.suptitle(f"Scatter-Gather: Makespan über die Chunk-Zahl ({TITEL})", y=1.02)
fig.tight_layout()
fig.savefig(OUT / "03_makespan_scatter_gather.png", bbox_inches="tight")
plt.close(fig)

# --- 4. Startspreizung (nur Scatter-Gather) -------------------------------
fig, ax = plt.subplots(figsize=(7.5, 4))
for i, s in enumerate(SYS):
    for j, c in enumerate(["2", "4"]):
        v = statistics.median(
            [float(coo[(s, "scatter_gather", w, c)]["wms_start_spread_s"]) for w in WL])
        x = i + (j - 0.5) * 0.34
        ax.bar(x, v, 0.32, color=FARBE[s], alpha=1.0 if c == "4" else 0.45)
        lbl = f"{v*1000:.0f} ms" if v < 1 else f"{v:.2f} s".replace(".", ",")
        ax.text(x, v * 1.15, lbl, ha="center", fontsize=7.5)
ax.set_yscale("log")
ax.set_xticks(range(len(SYS))); ax.set_xticklabels([NAME[s] for s in SYS])
ax.set_ylabel("Startspreizung [s], logarithmisch")
ax.set_title(f"Startspreizung der parallelen Zweige, Median über Workloads ({TITEL})")
ax.yaxis.set_major_formatter(FMT)
ax.legend(handles=[Patch(facecolor="grey", alpha=0.45, label="2 Chunks"),
                   Patch(facecolor="grey", label="4 Chunks")], fontsize=9)
fig.tight_layout(); fig.savefig(OUT / "04_startspreizung.png"); plt.close(fig)

print(f"fertig: {sorted(p.name for p in OUT.glob('*.png'))} in {OUT}")
