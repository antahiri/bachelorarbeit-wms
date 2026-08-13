#!/usr/bin/env python3
"""Erzeugt die beiden Abbildungen der AiiDA-Konfigurationsstudie (Kapitel 5.7)
aus den drei GETEILTEN Messreihen (gleicher Knotenmodus fuer alle drei):

  SQLite,  /fshpc        -> messungen/mogon/messungen-mogon-sha/aiida_sqlite/   (Job 433681)
  SQLite,  lokaler Scratch -> messungen/mogon/mogon-ergebnisse_aiida_scratch/aiida/ (Job 435095)
  PostgreSQL, /fshpc     -> messungen/mogon/messungen-mogon-sha/aiida_psql/aiida/

Ausfuehren im Wurzelverzeichnis des Repositories:
    python3 make_plots_aiida_konfig.py
Ausgabe: auswertung/aiida_scratch_fshpc_psql/06_... und 07_...
"""

import csv
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(".")
OUT = ROOT / "auswertung" / "aiida_scratch_fshpc_psql"
OUT.mkdir(parents=True, exist_ok=True)

SERIEN = {
    "SQLite,\nlokaler Scratch": {
        "central": ROOT / "messungen/mogon/mogon-ergebnisse_aiida_scratch/aiida/aiida_central_results_scratch.csv",
        "coord":   ROOT / "messungen/mogon/mogon-ergebnisse_aiida_scratch/aiida/aiida_coordination_results_scratch.csv",
        "farbe": "#1db584",
    },
    "PostgreSQL,\n/fshpc": {
        "central": ROOT / "messungen/mogon/messungen-mogon-sha/aiida_psql/aiida/aiida_central_results.csv",
        "coord":   ROOT / "messungen/mogon/messungen-mogon-sha/aiida_psql/aiida/aiida_coordination_results.csv",
        "farbe": "#3b82f6",
    },
    "SQLite,\n/fshpc": {
        "central": ROOT / "messungen/mogon/messungen-mogon-sha/aiida_sqlite/aiida_central_results_sha.csv",
        "coord":   ROOT / "messungen/mogon/messungen-mogon-sha/aiida_sqlite/aiida_coordination_results_sha.csv",
        "farbe": "#ef4444",
    },
}

KONFIGS = [("pipeline", "short", "1"), ("pipeline", "medium", "1"), ("pipeline", "long", "1"),
           ("scatter_gather", "short", "1"), ("scatter_gather", "short", "2"), ("scatter_gather", "short", "4"),
           ("scatter_gather", "medium", "1"), ("scatter_gather", "medium", "2"), ("scatter_gather", "medium", "4"),
           ("scatter_gather", "long", "1"), ("scatter_gather", "long", "2"), ("scatter_gather", "long", "4")]

XLABELS = ["P\nsho\nc1", "P\nmed\nc1", "P\nlon\nc1",
           "SG\nsho\nc1", "SG\nsho\nc2", "SG\nsho\nc4",
           "SG\nmed\nc1", "SG\nmed\nc2", "SG\nmed\nc4",
           "SG\nlon\nc1", "SG\nlon\nc2", "SG\nlon\nc4"]


def lese_central(pfad):
    werte = {}
    with open(pfad) as f:
        for r in csv.DictReader(f):
            if r["system"] != "aiida":
                continue
            werte[(r["pattern"], r["workload"], r["chunks"])] = float(r["overhead_s"])
    return werte


def lese_coord(pfad):
    zeilen = []
    with open(pfad) as f:
        for r in csv.DictReader(f):
            if r["system"] == "aiida":
                zeilen.append(r)
    return zeilen


# ---------- Abbildung 06: Overhead ueber alle 12 Konfigurationen ----------
fig, ax = plt.subplots(figsize=(12.5, 5.2), dpi=120)
breite = 0.26
for i, (name, s) in enumerate(SERIEN.items()):
    werte = lese_central(s["central"])
    ys = [werte[k] for k in KONFIGS]
    xs = [x + (i - 1) * breite for x in range(len(KONFIGS))]
    ax.bar(xs, ys, width=breite, label=name.replace("\n", " "), color=s["farbe"])

ax.set_xticks(range(len(KONFIGS)))
ax.set_xticklabels(XLABELS, fontsize=9)
ax.set_ylabel("Overhead [s]")
ax.set_title("AiiDA: Overhead nach Datenbank und Ablageort (MOGON, geteilter Knoten)")
ax.legend(loc="upper left", ncols=3)
ax.grid(axis="y", alpha=0.3)
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
fig.savefig(OUT / "06_aiida_konfigurationen_overhead.png")
plt.close(fig)
print("06 geschrieben:", OUT / "06_aiida_konfigurationen_overhead.png")

# ---------- Abbildung 07: Koordinationskosten (SG, 4 Chunks) ----------
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.6), dpi=120)
namen = list(SERIEN.keys())
farben = [SERIEN[n]["farbe"] for n in namen]

spreiz, split = [], []
for name in namen:
    zeilen = lese_coord(SERIEN[name]["coord"])
    sp = [float(r["wms_start_spread_s"]) for r in zeilen
          if r["pattern"] == "scatter_gather" and r["chunks"] == "4" and r["wms_start_spread_s"]]
    sc = [float(r["wms_comp_to_agg_s"]) for r in zeilen
          if r["pattern"] == "scatter_gather" and r["chunks"] == "4" and r["wms_comp_to_agg_s"]]
    spreiz.append(statistics.median(sp))
    split.append(statistics.median(sc))

for ax, ys, titel in ((ax1, spreiz, "Startspreizung der vier Zweige"),
                      (ax2, split, "Übergang compute \u2192 aggregate")):
    ax.bar(namen, ys, color=farben, width=0.55)
    ax.set_title(titel)
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    for x, y in zip(namen, ys):
        ax.annotate(f"{y:.2f} s".replace(".", ","), (x, y),
                    ha="center", va="bottom", fontsize=10)
ax1.set_ylabel("Zeit [s], Median über Workloads")
fig.suptitle("AiiDA: Koordinationskosten je Konfiguration (Scatter-Gather, 4 Chunks, geteilter Knoten)")
fig.tight_layout()
fig.savefig(OUT / "07_aiida_konfigurationen_koordination.png")
plt.close(fig)
print("07 geschrieben:", OUT / "07_aiida_konfigurationen_koordination.png")
