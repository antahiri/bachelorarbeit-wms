#!/usr/bin/env python3
"""Uebergangslatenzen zwischen aufeinanderfolgenden Tasks, Systemvergleich.

Erzeugt drei Abbildungen aus den coordination_results der dedizierten
Messungen und legt sie automatisch im Unterordner uebergangslatenzen/
neben diesem Skript ab (Konvention wie konfigurationsstudie/):

  05_lokal_uebergangslatenzen.png   lokal, fuenf Systeme
  06_uebergangslatenzen.png         MOGON exklusiv, vier Systeme
  07_delta_uebergangslatenzen.png   exklusiv minus geteilt

Die Eingabedateien werden unterhalb der Projektwurzel automatisch
gesucht (Wurzel = Ordner ueber dem Skript, alternativ als Argument:
python3 make_uebergangslatenzen.py /pfad/zur/wurzel):

  */lokal_mac/coordination_results.csv          (Sammel lokal)
  */mogon_exc/coordination_results.csv          (Sammel exklusiv)
  */mogon_sha/coordination_results.csv          (Sammel geteilt)
  */lokal_mac/*/aiida/aiida_coordination_results.csv  (AiiDA psql lokal)
  */aiida_psql/*exc*  bzw. in messungen-mogon-exc     (AiiDA psql exklusiv)
  */aiida_psql/*sha*  bzw. in messungen-mogon-sha     (AiiDA psql geteilt)

AiiDA stammt in allen drei Bildern aus den expliziten
PostgreSQL-Einzeldateien, denn die Sammel-CSVs enthalten fuer AiiDA
auf MOGON die SQLite-auf-/fshpc-Daten der Konfigurationsstudie.
Ordner mit smoke oder sqlite im Pfad werden bei der AiiDA-Suche
ausgeschlossen. Das Delta rechnet mit dem PostgreSQL-Paar.

Niveau-Bilder: zwei Panels (Pipeline sowie Scatter-Gather mit 1, 2
und 4 Chunks, c1 blass, c2 mittel, c4 voll), je Uebergang der Median
ueber die drei Workloads, logarithmische Achse. AiiDA auf MOGON
stammt aus der PostgreSQL-Messung (central-Median 12,82 s), denn die
Sammel-CSV der exklusiven Kampagne enthaelt fuer AiiDA die
SQLite-auf-/fshpc-Daten der Konfigurationsstudie.

Delta-Bild: je Konfiguration erst der Median ueber deren Uebergaenge,
dann die gepaarte Differenz exklusiv minus geteilt, zwoelf Punkte je
System wie im Overhead-Delta. AiiDA nutzt dort das SQLite-Paar aus
beiden Sammel-CSVs, weil nur diese Konfiguration in beiden Modi
gemessen wurde (kein geteilter PostgreSQL-Lauf). Die
Bildunterschriften muessen das jeweils deklarieren.
"""

import csv
import statistics
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter

SKRIPT_DIR = Path(__file__).resolve().parent
WURZEL = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 \
    else SKRIPT_DIR.parent
OUT = SKRIPT_DIR / "uebergangslatenzen"

FARBE = {"streamflow": "#0069C0", "nextflow": "#FF7A00", "merlin": "#00A651",
         "aiida": "#E4002B", "pegasus": "#8E44AD"}
NAME = {"streamflow": "StreamFlow", "nextflow": "Nextflow", "merlin": "Merlin",
        "aiida": "AiiDA", "pegasus": "Pegasus"}
WL = ["short", "medium", "long"]

PIPE_FELDER = [("wms_gen_to_pre_s", "gen \u2192 pre"),
               ("wms_pre_to_comp_s", "pre \u2192 comp"),
               ("wms_comp_to_post_s", "comp \u2192 post")]
SG_FELDER = [("wms_gen_to_pre_s", "gen \u2192 pre"),
             ("wms_pre_to_split_s", "pre \u2192 split"),
             ("wms_split_to_comp_s", "split \u2192 comp"),
             ("wms_comp_to_agg_s", "comp \u2192 agg"),
             ("wms_agg_to_post_s", "agg \u2192 post")]

KONF = [("pipeline", w, "1") for w in WL] + \
       [("scatter_gather", w, c) for w in WL for c in ["1", "2", "4"]]

plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": 0.3,
                     "axes.axisbelow": True, "figure.dpi": 150})


def komma(x, pos=None):
    return f"{x:g}".replace(".", ",")


FMT = FuncFormatter(komma)


def finde(muster, muss=(), ohne=("smoke",), pflicht=True):
    """Sucht eine Datei unterhalb der Wurzel.

    muss: Zeichenketten, die im Pfad vorkommen muessen (klein).
    Das trennt gleichnamige Dateien in verschiedenen Ordnern, etwa
    aiida_coordination_results.csv lokal gegenueber MOGON.
    ohne: Zeichenketten, die im Pfad nicht vorkommen duerfen,
    standardmaessig smoke (Vorabtests mit nur einer Wiederholung).
    Bei Mehrdeutigkeit bricht das Skript ab, statt stillschweigend
    die falsche Datei zu nehmen.
    """
    treffer = sorted(p for p in WURZEL.rglob(muster)
                     if OUT not in p.parents
                     and all(m in str(p).lower() for m in muss)
                     and not any(x in str(p).lower() for x in ohne))
    if not treffer:
        if not pflicht:
            return None
        sys.exit(f"FEHLER: '{muster}' mit {list(muss)} wurde unterhalb "
                 f"von {WURZEL} nicht gefunden. Wurzel als Argument "
                 "angeben oder Datei dorthin legen.")
    if len(treffer) > 1:
        print(f"FEHLER: mehrere Treffer fuer '{muster}' mit {list(muss)}:")
        for p in treffer:
            print(f"  {p}")
        sys.exit("Bitte eindeutig machen, es wird nichts geraten.")
    return treffer[0]


def load(pfad):
    # Merlin schreibt in den MOGON-CSVs "Merlin" / "Pipeline" /
    # "Scatter-Gather", daher werden system und pattern normalisiert.
    daten = {}
    for r in csv.DictReader(open(pfad)):
        schluessel = (r["system"].lower(),
                      r["pattern"].lower().replace("-", "_"),
                      r["workload"], r["chunks"])
        daten[schluessel] = r
    return daten


def med(coo, s, pattern, chunks, feld):
    return statistics.median(
        [float(coo[(s, pattern, w, chunks)][feld]) for w in WL])


def konfig_latenz(coo, s, pattern, w, chunks):
    """Median der Uebergangslatenzen einer Konfiguration."""
    felder = PIPE_FELDER if pattern == "pipeline" else SG_FELDER
    zeile = coo[(s, pattern, w, chunks)]
    return statistics.median([float(zeile[feld]) for feld, _ in felder])


def zeichne(coo, systeme, titel, ausgabe):
    fig, axes = plt.subplots(1, 2, figsize=(12, 3.8),
                             gridspec_kw={"width_ratios": [3, 5]})
    slot = 0.8 / len(systeme)

    # Panel 1: Pipeline (ein Balken je System und Uebergang).
    axp = axes[0]
    for i, s in enumerate(systeme):
        werte = [med(coo, s, "pipeline", "1", feld)
                 for feld, _ in PIPE_FELDER]
        x = [j + (i - (len(systeme) - 1) / 2) * slot
             for j in range(len(PIPE_FELDER))]
        axp.bar(x, werte, slot * 0.9, color=FARBE[s])
    axp.set_title("Pipeline", fontsize=10)
    axp.set_xticks(range(len(PIPE_FELDER)))
    axp.set_xticklabels([label for _, label in PIPE_FELDER], fontsize=8)

    # Panel 2: Scatter-Gather, je System drei Balken
    # (c1 blass, c2 mittel, c4 voll).
    axs = axes[1]
    stufen = [("1", 0.35), ("2", 0.65), ("4", 1.0)]
    for i, s in enumerate(systeme):
        for k, (c, alpha) in enumerate(stufen):
            werte = [med(coo, s, "scatter_gather", c, feld)
                     for feld, _ in SG_FELDER]
            x = [j + (i - (len(systeme) - 1) / 2) * slot
                 + (k - 1) * slot * 0.31
                 for j in range(len(SG_FELDER))]
            axs.bar(x, werte, slot * 0.29, color=FARBE[s], alpha=alpha)
    axs.set_title("Scatter-Gather (1, 2 und 4 Chunks)", fontsize=10)
    axs.set_xticks(range(len(SG_FELDER)))
    axs.set_xticklabels([label for _, label in SG_FELDER], fontsize=8)

    for axi in axes:
        axi.set_yscale("log")
        axi.yaxis.set_major_formatter(FMT)
    axes[0].set_ylabel("\u00dcbergangslatenz [s], logarithmisch")

    fig.suptitle(titel, y=1.02)
    handles = [Patch(facecolor=FARBE[s], label=NAME[s]) for s in systeme]
    handles += [Patch(facecolor="grey", alpha=0.35, label="1 Chunk"),
                Patch(facecolor="grey", alpha=0.65, label="2 Chunks"),
                Patch(facecolor="grey", label="4 Chunks")]
    fig.legend(handles=handles, ncol=len(handles), fontsize=8.5,
               loc="lower center", bbox_to_anchor=(0.5, -0.06))
    fig.tight_layout()
    fig.savefig(OUT / ausgabe, bbox_inches="tight")
    plt.close(fig)

    print(f"{ausgabe} (Median ueber Workloads und Uebergaenge):")
    for s in systeme:
        p = statistics.median(
            [med(coo, s, "pipeline", "1", f) for f, _ in PIPE_FELDER])
        sg = {c: statistics.median(
            [med(coo, s, "scatter_gather", c, f) for f, _ in SG_FELDER])
            for c in ("1", "2", "4")}
        alle = ([med(coo, s, "pipeline", "1", f) for f, _ in PIPE_FELDER] +
                [med(coo, s, "scatter_gather", c, f)
                 for c in ("1", "2", "4") for f, _ in SG_FELDER])
        print(f"  {NAME[s]:<11} P {p:6.2f} | SG c1 {sg['1']:6.2f}"
              f" | c2 {sg['2']:6.2f} | c4 {sg['4']:6.2f}"
              f"   Spanne {min(alle):.2f} bis {max(alle):.2f} s")


def zeichne_delta(coo_e, coo_s, systeme, titel, ausgabe):
    fig, ax = plt.subplots(figsize=(8, 4.2))
    print(f"{ausgabe} (Delta exklusiv minus geteilt, 12 Konfigurationen):")
    for i, s in enumerate(systeme):
        d = [konfig_latenz(coo_e, s, p, w, c) -
             konfig_latenz(coo_s, s, p, w, c) for p, w, c in KONF]
        ax.scatter([i] * len(d), d, color=FARBE[s], alpha=0.6, s=28,
                   zorder=3)
        m = statistics.median(d)
        ax.plot([i - 0.25, i + 0.25], [m, m], color=FARBE[s], lw=2.5,
                zorder=4)
        mr = round(m, 2)
        if mr == 0:
            mr = 0.0
        ax.text(i + 0.3, m, f"{mr:+.2f} s".replace(".", ","), fontsize=8,
                va="center", color=FARBE[s])
        print(f"  {NAME[s]:<11} Median {m:+6.2f} s"
              f"   Spanne {min(d):+.2f} bis {max(d):+.2f} s")
    ax.axhline(0, color="black", lw=0.8)
    ax.set_xticks(range(len(systeme)))
    ax.set_xticklabels([NAME[s] for s in systeme])
    ax.set_ylabel("Latenzdifferenz [s]")
    ax.set_title(titel, fontsize=10)
    ax.yaxis.set_major_formatter(FMT)
    fig.tight_layout()
    fig.savefig(OUT / ausgabe, bbox_inches="tight")
    plt.close(fig)


def main():
    lokal = finde("coordination_results.csv", ("lokal_mac",))
    exc = finde("coordination_results.csv", ("mogon_exc",))
    sha = finde("coordination_results.csv", ("mogon_sha",))
    # AiiDA kommt in allen drei Bildern aus den expliziten
    # PostgreSQL-Einzeldateien. Gleichnamige Dateien existieren in
    # den SQLite-Ordnern, daher Pfadmarker und Ausschluesse.
    aiida_lokal = finde("aiida_coordination_results.csv",
                        ("lokal_mac",), ohne=("smoke", "sqlite"))
    psql_exc = finde("aiida_coordination_results*.csv",
                     ("aiida_psql", "exc"))
    psql_sha = finde("aiida_coordination_results*.csv",
                     ("aiida_psql", "sha"))

    OUT.mkdir(parents=True, exist_ok=True)
    print("Eingaben (Sammel-CSVs plus explizite AiiDA-PostgreSQL-Dateien):")
    for p in (lokal, exc, sha, aiida_lokal, psql_exc, psql_sha):
        print(f"  {p}")
    print(f"Ausgabeordner: {OUT}\n")

    def mit_aiida(sammel, aiida_pfad):
        """Sammel-CSV mit den AiiDA-Zeilen der psql-Datei ueberschrieben."""
        daten = dict(sammel)
        for schluessel, zeile in load(aiida_pfad).items():
            if schluessel[0] == "aiida":
                daten[schluessel] = zeile
        return daten

    zeichne(mit_aiida(load(lokal), aiida_lokal),
            ["streamflow", "nextflow", "merlin", "aiida", "pegasus"],
            "\u00dcbergangslatenzen zwischen aufeinanderfolgenden Tasks "
            "(Median \u00fcber Workloads, lokal, MacBook)",
            "05_lokal_uebergangslatenzen.png")

    coo_exc = mit_aiida(load(exc), psql_exc)
    zeichne(coo_exc,
            ["streamflow", "nextflow", "merlin", "aiida"],
            "\u00dcbergangslatenzen zwischen aufeinanderfolgenden Tasks "
            "(Median \u00fcber Workloads, MOGON, exklusiv)",
            "06_uebergangslatenzen.png")

    # Delta: gepaarte Differenz je Konfiguration, AiiDA als
    # PostgreSQL-Paar (exklusiv wie geteilt aus den aiida_psql-Laeufen).
    zeichne_delta(coo_exc, mit_aiida(load(sha), psql_sha),
                  ["streamflow", "nextflow", "merlin", "aiida"],
                  "\u00dcbergangslatenzen: exklusiver minus geteilter "
                  "Knoten, je Konfiguration und Median",
                  "07_delta_uebergangslatenzen.png")

    print("\nFertig. Drei Bilder liegen in:")
    print(f"  {OUT}")
    print("AiiDA-Quellen: lokal PostgreSQL, MOGON exklusiv PostgreSQL, "
          "Delta PostgreSQL-Paar. Fuer die Bildunterschriften.")


if __name__ == "__main__":
    main()
