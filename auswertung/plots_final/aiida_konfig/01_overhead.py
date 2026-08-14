"""Overhead von AiiDA nach Datenbank und Ablageort der Datenhaltung,
MOGON im geteilten Betrieb. Drei Konfigurationen je Konfiguration des
Workflows, Hoehe ist der Median der paarweisen Differenzen aus v2.

Aufruf aus dem Repo-Wurzelverzeichnis:
    python3 auswertung/plots_final/aiida_konfig/01_overhead.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_central, speichere, dezimal,
                    WORKLOADS, WORKLOAD_NAME)
import matplotlib.pyplot as plt

stil()
zentral = lade_central("aiida_konfig")
VARIANTEN = [
    ("aiida_sqlite_scratch", "SQLite, lokaler Scratch", "#10B981"),
    ("aiida_psql_fshpc",     "PostgreSQL, /fshpc",      "#3B82F6"),
    ("aiida_sqlite_fshpc",   "SQLite, /fshpc",          "#EF4444"),
]

konfigs = [("pipeline", w, 1) for w in WORKLOADS]
konfigs += [("scatter_gather", w, c) for w in WORKLOADS for c in (1, 2, 4)]

def beschriftung(k):
    muster = "P" if k[0] == "pipeline" else "SG"
    return f"{muster}\n{WORKLOAD_NAME[k[1]]}\nc{k[2]}"

fig, ax = plt.subplots(figsize=(10.5, 4.2))
breite = 0.26
for j, (v, name, farbe) in enumerate(VARIANTEN):
    werte = [zentral[(v, *k)]["overhead_s"] for k in konfigs]
    x = [i + (j - 1) * breite for i in range(len(konfigs))]
    ax.bar(x, werte, width=breite, color=farbe, label=name)

ax.set_xticks(range(len(konfigs)))
ax.set_xticklabels([beschriftung(k) for k in konfigs])
ax.set_ylabel("Overhead in s")
ax.legend(ncol=len(VARIANTEN), loc="upper left", frameon=True)
ax.margins(y=0.16)
dezimal(ax)

speichere(fig, "aiida_konfig", "06_aiida_konfigurationen_overhead.png")
