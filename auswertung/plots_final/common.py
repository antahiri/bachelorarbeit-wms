"""Gemeinsame Grundlagen aller finalen Plots.

Liest ausschliesslich messungen/finale_Ergebnisse_v2 und schreibt die
PNGs direkt in die LaTeX-Abbildungsordner unter content/figs/, mit den
bisherigen Dateinamen. Regeln aus dem Betreuer-Feedback: keine Titel im
Bild, einheitliche Schriftgroessen, log-Achsen mit Basisangabe.
"""
import csv
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

REPO = Path(__file__).resolve().parents[2]
V2   = REPO / "messungen" / "finale_Ergebnisse_v2"
AUSGABE = REPO / "auswertung"
FIGS = {
    "lokal":        AUSGABE / "lokal_mac_final",
    "mogon":        AUSGABE / "mogon_exc_final",
    "aiida_konfig": AUSGABE / "aiida_konfig_final",
}
CENTRAL = {
    "lokal_mac":  V2 / "lokal_mac" / "central_results.csv",
    "mogon_exc":  V2 / "mogon_exc" / "mogon_central_results.csv",
    "mogon_sha":  V2 / "mogon_sha" / "central_results.csv",
    "aiida_konfig": V2 / "aiida_konfig" / "central_results.csv",
}

REIHENFOLGE = ["streamflow", "nextflow", "merlin", "aiida", "pegasus"]
NAME  = {"streamflow": "StreamFlow", "nextflow": "Nextflow",
         "merlin": "Merlin", "aiida": "AiiDA", "pegasus": "Pegasus"}
FARBE = {"streamflow": "#3B82F6", "nextflow": "#F59E0B",
         "merlin": "#10B981", "aiida": "#EF4444", "pegasus": "#8B5CF6"}

WORKLOADS = ["short", "medium", "long"]
WORKLOAD_NAME = {"short": "kurz", "medium": "mittel", "long": "lang"}

GROESSE_VOLL = (7.0, 3.9)   # Abbildung ueber die volle Textbreite
GROESSE_HALB = (4.7, 3.5)   # Subfigure mit 0,49 Textbreite
GROESSE_DELTA = (5.6, 3.5)  # beide Delta-Boxplots, identische Groesse

# Uebergaenge je Muster in den Koordinationsdateien
UEBERGAENGE = {
    "pipeline": ["wms_gen_to_pre_s", "wms_pre_to_comp_s", "wms_comp_to_post_s"],
    "scatter_gather": ["wms_gen_to_pre_s", "wms_pre_to_split_s",
                       "wms_split_to_comp_s", "wms_comp_to_agg_s",
                       "wms_agg_to_post_s"],
}

def stil():
    """Einheitliche Darstellung fuer alle Plots (keine Titel im Bild)."""
    plt.rcParams.update({
        "font.size": 10, "axes.labelsize": 10,
        "xtick.labelsize": 9, "ytick.labelsize": 9, "legend.fontsize": 9,
        "axes.grid": True, "grid.alpha": 0.25, "axes.axisbelow": True,
        "figure.dpi": 300, "savefig.bbox": "tight",
    })

def komma(x, pos=None):
    return f"{x:g}".replace(".", ",")

def komma_vorzeichen(x, pos=None):
    return f"{x:+g}".replace(".", ",")

def dezimal(ax, achse="y", vorzeichen=False):
    f = FuncFormatter(komma_vorzeichen if vorzeichen else komma)
    (ax.yaxis if achse == "y" else ax.xaxis).set_major_formatter(f)

def log10_y(ax, label):
    """Logarithmische y-Achse mit Basisangabe in der Beschriftung."""
    ax.set_yscale("log")
    ax.set_ylabel(f"{label} (log$_{{10}}$)")

def lade_central(ebene):
    """Zeilen der zentralen v2-Datei als Dict je Konfiguration."""
    out = {}
    for r in csv.DictReader(CENTRAL[ebene].open()):
        k = (r["system"], r["pattern"], r["workload"], int(r["chunks"]))
        out[k] = {sp: float(r[sp]) for sp in
                  ("ref_makespan_s", "wms_makespan_s", "overhead_s")}
    return out

def lade_koordination(ebene, datei=None):
    """Koordinationsdatei (Mediane je Konfiguration) als Dict."""
    ordner = CENTRAL[ebene].parent
    pfad = ordner / datei if datei else next(ordner.glob("*coordination*"))
    kopf = pfad.open().readline()
    trenn = ";" if kopf.count(";") > kopf.count(",") else ","
    out = {}
    for r in csv.DictReader(pfad.open(), delimiter=trenn):
        k = (r["system"], r["pattern"].strip().lower().replace("-", "_"),
             r["workload"], int(r["chunks"]))
        out[k] = {sp: (float(v) if v not in ("", None) else None)
                  for sp, v in r.items()
                  if sp not in ("system", "pattern", "workload", "chunks")}
    return out

def speichere(fig, gruppe, dateiname):
    ziel = FIGS[gruppe]
    ziel.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(ziel / dateiname)
    print("geschrieben:", ziel / dateiname)
