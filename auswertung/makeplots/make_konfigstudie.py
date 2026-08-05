#!/usr/bin/env python3
"""Konfigurationsstudie AiiDA: Backend und Speicherort (MOGON, exklusiv).
Drei Varianten: PostgreSQL auf /fshpc, SQLite auf /fshpc, SQLite auf Scratch.
Kontrollierte Paare: Backend (psql vs sqlite, beide fshpc),
Speicherort (fshpc vs Scratch, beide sqlite)."""

import csv, statistics
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

MESS = Path.home() / "bachelorarbeit" / "messungen" / "mogon"
OUT = Path.home() / "bachelorarbeit" / "auswertung" / "konfigurationsstudie"
OUT.mkdir(parents=True, exist_ok=True)

FILES = {
    "psql_fshpc": (
        MESS / "messungen-mogon-exc" / "aiida_psql" / "aiida_central_results.csv",
        MESS / "messungen-mogon-exc" / "aiida_psql" / "aiida_coordination_results.csv"),
    "sqlite_fshpc": (
        MESS / "messungen-mogon-exc" / "aiida_sqlite_fshpc" / "aiida_central_results_exc_fshpc_sqlite.csv",
        MESS / "messungen-mogon-exc" / "aiida_sqlite_fshpc" / "aiida_coordination_results_exc_fshpc_sqlite.csv"),
    "sqlite_scratch": (
        MESS / "mogon-ergebnisse_aiida_scratch" / "aiida" / "aiida_central_results_scratch.csv",
        MESS / "mogon-ergebnisse_aiida_scratch" / "aiida" / "aiida_coordination_results_scratch.csv"),
}

VAR = [("sqlite_scratch", "SQLite,\nlokaler Scratch", "#10B981"),
       ("psql_fshpc", "PostgreSQL,\n/fshpc", "#3B82F6"),
       ("sqlite_fshpc", "SQLite,\n/fshpc", "#EF4444")]

plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": 0.25,
                     "axes.axisbelow": True, "figure.dpi": 150,
                     "axes.spines.top": False, "axes.spines.right": False})

def komma(x, pos=None):
    return f"{x:g}".replace(".", ",")
FMT = FuncFormatter(komma)

def load(name, kind):
    idx = 0 if kind == "central" else 1
    pfad = FILES[name][idx]
    if not pfad.is_file():
        raise SystemExit(f"Datei fehlt: {pfad}")
    return {(r["pattern"], r["workload"], r["chunks"]): r
            for r in csv.DictReader(open(pfad, encoding="utf-8-sig"))}

cen = {v: load(v, "central") for v, _, _ in VAR}
coo = {v: load(v, "coordination") for v, _, _ in VAR}

WL = ["short", "medium", "long"]
KONF = [("pipeline", w, "1") for w in WL] + \
       [("scatter_gather", w, c) for w in WL for c in ["1", "2", "4"]]

# --- 1. Overhead ueber alle Konfigurationen -------------------------------
fig, ax = plt.subplots(figsize=(10, 4.2))
bw = 0.26
for j, (v, label, farbe) in enumerate(VAR):
    werte = [float(cen[v][k]["overhead_s"]) for k in KONF]
    x = [i + (j - 1) * bw for i in range(len(KONF))]
    ax.bar(x, werte, bw, color=farbe, label=label.replace("\n", " "))
ax.set_xticks(range(len(KONF)))
ax.set_xticklabels([f"{'P' if p == 'pipeline' else 'SG'}\n{w[:3]}\nc{c}"
                    for p, w, c in KONF], fontsize=7)
ax.set_ylabel("Overhead [s]")
ax.yaxis.set_major_formatter(FMT)
ax.set_title("AiiDA: Overhead nach Backend und Speicherort (MOGON, exklusiv)")
ax.legend(fontsize=8.5, ncol=3)
fig.tight_layout()
fig.savefig(OUT / "06_aiida_konfigurationen_overhead.png")
plt.close(fig)

# --- 2. Koordination: Startspreizung und Uebergang (c4, Median) -----------
fig, axes = plt.subplots(1, 2, figsize=(9, 3.8))
metriken = [("wms_start_spread_s", "Startspreizung der vier Zweige"),
            ("wms_split_to_comp_s", "Übergang split → erster compute")]
for axi, (feld, titel) in zip(axes, metriken):
    for j, (v, label, farbe) in enumerate(VAR):
        m = statistics.median(
            [float(coo[v][("scatter_gather", w, "4")][feld]) for w in WL])
        axi.bar(j, m, 0.62, color=farbe)
        axi.text(j, m * 1.03, f"{m:.2f} s".replace(".", ","),
                 ha="center", fontsize=9)
    axi.set_xticks(range(3))
    axi.set_xticklabels([lab for _, lab, _ in VAR], fontsize=8.5)
    axi.set_title(titel, fontsize=10)
    axi.yaxis.set_major_formatter(FMT)
axes[0].set_ylabel("Zeit [s], Median über Workloads")
fig.suptitle("AiiDA: Koordinationskosten je Konfiguration (Scatter-Gather, 4 Chunks)",
             y=1.03)
fig.tight_layout()
fig.savefig(OUT / "07_aiida_konfigurationen_koordination.png",
            bbox_inches="tight")
plt.close(fig)

# --- Kennzahlen fuer den Text ---------------------------------------------
print("Median-Overhead je Variante (12 Konfigurationen):")
for v, label, _ in VAR:
    werte = [float(cen[v][k]["overhead_s"]) for k in KONF]
    print(f"  {label.replace(chr(10), ' '):24} {statistics.median(werte):6.2f} s"
          f"   ({min(werte):5.2f} bis {max(werte):5.2f})")
print("\nKontrollierte Faktoren (Median der 12 Verhaeltnisse):")
back = statistics.median([float(cen['sqlite_fshpc'][k]['overhead_s']) /
                          float(cen['psql_fshpc'][k]['overhead_s']) for k in KONF])
ort = statistics.median([float(cen['sqlite_fshpc'][k]['overhead_s']) /
                         float(cen['sqlite_scratch'][k]['overhead_s']) for k in KONF])
print(f"  Backend  (sqlite/psql, beide fshpc):       {back:.2f}")
print(f"  Speicherort (fshpc/scratch, beide sqlite): {ort:.2f}")
