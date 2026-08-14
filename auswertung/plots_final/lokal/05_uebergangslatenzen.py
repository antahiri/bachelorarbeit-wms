"""Uebergangslatenzen zwischen aufeinanderfolgenden Schritten, lokale Ebene. Linkes Panel Pipeline, rechtes Panel
Scatter-Gather mit 1, 2 und 4 Teilstuecken (aufsteigende Saettigung).
Balkenhoehe ist der Median ueber die drei Rechenlasten,
logarithmische Achse.

Aufruf aus dem Repo-Wurzelverzeichnis:
    python3 auswertung/plots_final/lokal/05_uebergangslatenzen.py
"""
import sys
import statistics
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_koordination, speichere, komma,
                    NAME, FARBE, WORKLOADS, UEBERGAENGE)
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
from matplotlib.ticker import FuncFormatter

stil()
koord = lade_koordination("lokal_mac")
SYS = ["streamflow", "nextflow", "merlin", "aiida", "pegasus"]
PFEIL = {"wms_gen_to_pre_s": "gen \u2192 pre",
         "wms_pre_to_comp_s": "pre \u2192 comp",
         "wms_comp_to_post_s": "comp \u2192 post",
         "wms_pre_to_split_s": "pre \u2192 split",
         "wms_split_to_comp_s": "split \u2192 comp",
         "wms_comp_to_agg_s": "comp \u2192 agg",
         "wms_agg_to_post_s": "agg \u2192 post"}
ALPHA = {1: 0.35, 2: 0.65, 4: 1.0}

fig, (ap, asg) = plt.subplots(
    1, 2, figsize=(11.5, 3.9), sharey=True,
    gridspec_kw={"width_ratios": [3, 5]})

# Pipeline: je Uebergang vier Systembalken (nur 1 Teilstueck)
breite = 0.17
felder = UEBERGAENGE["pipeline"]
for j, s in enumerate(SYS):
    for i, f in enumerate(felder):
        med = statistics.median(
            koord[(s, "pipeline", w, 1)][f] for w in WORKLOADS)
        ap.bar(i + (j - 2.0) * breite, med, width=breite, color=FARBE[s])
ap.set_xticks(range(len(felder)))
ap.set_xticklabels([PFEIL[f] for f in felder])
ap.set_xlabel("Pipeline")

# Scatter-Gather: je Uebergang und System drei Balken (1, 2, 4)
felder = UEBERGAENGE["scatter_gather"]
schmal = 0.058
for j, s in enumerate(SYS):
    for k, c in enumerate((1, 2, 4)):
        for i, f in enumerate(felder):
            med = statistics.median(
                koord[(s, "scatter_gather", w, c)][f] for w in WORKLOADS)
            x = i + (j - 2.0) * 3.2 * schmal + (k - 1) * schmal
            asg.bar(x, med, width=schmal,
                    color=to_rgba(FARBE[s], ALPHA[c]))
asg.set_xticks(range(len(felder)))
asg.set_xticklabels([PFEIL[f] for f in felder])
asg.set_xlabel("Scatter-Gather (1, 2 und 4 Teilstücke)")

ap.set_yscale("log")
ap.yaxis.set_major_formatter(FuncFormatter(komma))
ap.set_ylabel("Übergangslatenz in s (log$_{10}$)")
ap.margins(y=0.15)

handles = [plt.Rectangle((0, 0), 1, 1, color=FARBE[s]) for s in SYS]
handles += [plt.Rectangle((0, 0), 1, 1, color=to_rgba("#4B5563", ALPHA[c]))
            for c in (1, 2, 4)]
namen = [NAME[s] for s in SYS] + ["1 Teilstück", "2 Teilstücke", "4 Teilstücke"]
fig.legend(handles, namen, ncol=8, loc="lower center",
           bbox_to_anchor=(0.5, -0.04), frameon=True, fontsize=8.5)
fig.subplots_adjust(bottom=0.24)

speichere(fig, "lokal", "05_lokal_uebergangslatenzen.png")
