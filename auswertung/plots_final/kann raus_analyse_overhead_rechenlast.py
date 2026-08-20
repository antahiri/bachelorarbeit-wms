import csv
import statistics
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import stil, NAME, FARBE, WORKLOADS, WORKLOAD_NAME, komma
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "auswertung" / "analyse_final"

QUELLEN = {
    "lokal": {
        "streamflow": "lokal_mac/results/streamflow_local",
        "nextflow":   "lokal_mac/results/nextflow",
        "merlin":     "lokal_mac/results/merlin",
        "aiida":      "lokal_mac/results/aiida",
        "pegasus":    "lokal_mac/results/pegasus"},
    "MOGON exklusiv": {
        "streamflow": "mogon/messungen-mogon-exc/streamflow",
        "nextflow":   "mogon/messungen-mogon-exc/nextflow",
        "merlin":     "mogon/messungen-mogon-exc/merlin",
        "aiida":      "mogon/messungen-mogon-exc/aiida_psql"},
    "MOGON geteilt": {
        "streamflow": "mogon/messungen-mogon-sha/streamflow",
        "nextflow":   "mogon/messungen-mogon-sha/nextflow",
        "merlin":     "mogon/messungen-mogon-sha/merlin",
        "aiida":      "mogon/messungen-mogon-sha/aiida_psql/aiida"},
}

def lies_paare(rel):
    pfad = sorted((REPO / "messungen" / rel).glob("*raw*results*.csv"))[0]
    kopf = pfad.open().readline()
    trenn = ";" if kopf.count(";") > kopf.count(",") else ","
    zeilen = list(csv.DictReader(pfad.open(), delimiter=trenn))
    sp = zeilen[0].keys()
    ref = ("ref_makespan_s" if "ref_makespan_s" in sp
           else "reference_outer_runtime_seconds")
    wms = ("wms_makespan_s" if "wms_makespan_s" in sp
           else next(c for c in sp if c.endswith("_outer_runtime_seconds")
                     and not c.startswith("reference")))
    return [(z["workload"], float(z[wms]) - float(z[ref])) for z in zeilen]

stil()
fig, achsen = plt.subplots(1, 3, figsize=(11.5, 3.8), sharey=True)
versatz = 0.13

for ax, (ebene, sysd) in zip(achsen, QUELLEN.items()):
    for j, (s, rel) in enumerate(sysd.items()):
        paare = lies_paare(rel)
        xoff = (j - (len(sysd) - 1) / 2) * versatz
        mediane = []
        for i, w in enumerate(WORKLOADS):
            werte = [v for wl, v in paare if wl == w]
            ax.scatter([i + xoff] * len(werte), werte, s=14,
                       color=FARBE[s], alpha=0.35, edgecolors="none")
            mediane.append(statistics.median(werte))
        ax.plot([i + xoff for i in range(3)], mediane, color=FARBE[s],
                lw=1.6, marker="_", markersize=11, markeredgewidth=2,
                label=NAME[s])
    ax.set_yscale("log")
    ax.set_xticks(range(3))
    ax.set_xticklabels([WORKLOAD_NAME[w] for w in WORKLOADS])
    ax.set_xlabel(f"Rechenlast ({ebene})")
achsen[0].set_ylabel("Overhead in s (log$_{10}$)")
achsen[0].yaxis.set_major_formatter(FuncFormatter(komma))
achsen[0].legend(loc="upper left", frameon=True, fontsize=8)

OUT.mkdir(parents=True, exist_ok=True)
fig.tight_layout()
fig.savefig(OUT / "analyse_overhead_rechenlast.png")
print("geschrieben:", OUT / "analyse_overhead_rechenlast.png")
