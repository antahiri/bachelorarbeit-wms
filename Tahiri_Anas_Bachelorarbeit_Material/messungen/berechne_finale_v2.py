#!/usr/bin/env python3
import csv, statistics as st, shutil
from pathlib import Path

REPO = Path(".")
MESS = REPO / "messungen"
ALT  = MESS / "finale_Ergebnisse"
NEU  = MESS / "finale_Ergebnisse_v2"

# Reihenfolge der Rechenlasten in den Ausgabedateien
WL_ORDNUNG = {"short": 0, "medium": 1, "long": 2}

# Ebene -> Zielname der zentralen Datei, Quellordner je System (raw-Dateien)
EBENEN = {
    "lokal_mac": ("central_results.csv", {
        "streamflow": "lokal_mac/results/streamflow_local",
        "nextflow":   "lokal_mac/results/nextflow",
        "merlin":     "lokal_mac/results/merlin",
        "aiida":      "lokal_mac/results/aiida",            # PostgreSQL
        "pegasus":    "lokal_mac/results/pegasus",
    }),
    "mogon_exc": ("mogon_central_results.csv", {
        "streamflow": "mogon/messungen-mogon-exc/streamflow",
        "nextflow":   "mogon/messungen-mogon-exc/nextflow",
        "merlin":     "mogon/messungen-mogon-exc/merlin",
        "aiida":      "mogon/messungen-mogon-exc/aiida_psql",
    }),
    "mogon_sha": ("central_results.csv", {
        "streamflow": "mogon/messungen-mogon-sha/streamflow",
        "nextflow":   "mogon/messungen-mogon-sha/nextflow",
        "merlin":     "mogon/messungen-mogon-sha/merlin",
        "aiida":      "mogon/messungen-mogon-sha/aiida_psql/aiida",
    }),

    "aiida_konfig": ("central_results.csv", {
        "aiida_psql_fshpc":    "mogon/messungen-mogon-sha/aiida_psql/aiida",
        "aiida_sqlite_fshpc":  "mogon/messungen-mogon-sha/aiida_sqlite",
        "aiida_sqlite_scratch":"mogon/mogon-ergebnisse_aiida_scratch/aiida",
    }),
}

# Alte zentrale Dateien der Konfigurationsstudie (fuer den Alt-Neu-Vergleich,
ALT_KONFIG = {
    "aiida_psql_fshpc":     "mogon/messungen-mogon-sha/aiida_psql/aiida/aiida_central_results.csv",
    "aiida_sqlite_fshpc":   "mogon/messungen-mogon-sha/aiida_sqlite/aiida_central_results_sha.csv",
    "aiida_sqlite_scratch": "mogon/mogon-ergebnisse_aiida_scratch/aiida/aiida_central_results_scratch.csv",
}

def lies_raw(ordner: Path):
    """Findet die raw-Datei, erkennt Trennzeichen und Schema, liefert
    Zeilen als (pattern, workload, chunks, ref_s, wms_s)."""
    kandidaten = sorted(ordner.glob("*raw*results*.csv"))
    if not kandidaten:
        raise FileNotFoundError(f"keine raw-Datei in {ordner}")
    if len(kandidaten) > 1:
        raise RuntimeError(f"Mehrere raw-Dateien in {ordner}: "
                           + ", ".join(p.name for p in kandidaten))
    pfad = kandidaten[0]
    kopf = pfad.open().readline()
    trenn = ";" if kopf.count(";") > kopf.count(",") else ","
    zeilen = list(csv.DictReader(pfad.open(), delimiter=trenn))
    sp = zeilen[0].keys()
    pat  = "pattern" if "pattern" in sp else "workflow_pattern"
    ref  = "ref_makespan_s" if "ref_makespan_s" in sp else "reference_outer_runtime_seconds"
    wms  = ("wms_makespan_s" if "wms_makespan_s" in sp
            else next(c for c in sp if c.endswith("_outer_runtime_seconds")
                      and not c.startswith("reference")))
    out = []
    for z in zeilen:
        muster = z[pat].strip().lower().replace("-", "_")
        out.append((muster, z["workload"], int(z["chunks"]),
                    float(z[ref]), float(z[wms])))
    return pfad.name, out

def hauptlauf():
    NEU.mkdir(exist_ok=True)
    diffs = []
    for ebene, (zieldatei, systeme) in EBENEN.items():
        alt_pfad = ALT / ebene / zieldatei
        alt_ov = {}
        if alt_pfad.exists():
            for z in csv.DictReader(alt_pfad.open()):
                alt_ov[(z["system"], z["pattern"], z["workload"],
                        int(z["chunks"]))] = float(z["overhead_s"])
        elif ebene == "aiida_konfig":
            for system, rel in ALT_KONFIG.items():
                for z in csv.DictReader((MESS / rel).open()):
                    muster = z["pattern"].strip().lower().replace("-", "_")
                    alt_ov[(system, muster, z["workload"],
                            int(z["chunks"]))] = float(z["overhead_s"])
        zielordner = NEU / ebene
        zielordner.mkdir(exist_ok=True)
        zeilen_neu = []
        for system, rel in systeme.items():
            name, rohe = lies_raw(MESS / rel)
            gruppen = {}
            for pat, wl, ch, r, w in rohe:
                gruppen.setdefault((pat, wl, ch), []).append((r, w))
            for (pat, wl, ch), paare in sorted(
                    gruppen.items(),
                    key=lambda g: (g[0][0], WL_ORDNUNG.get(g[0][1], 9), g[0][2])):
                ref_med = st.median(r for r, _ in paare)
                wms_med = st.median(w for _, w in paare)
                ov      = st.median(w - r for r, w in paare)   # NEU
                zeilen_neu.append({
                    "system": system, "pattern": pat, "workload": wl,
                    "chunks": ch,
                    "ref_makespan_s": f"{ref_med:.6f}",
                    "wms_makespan_s": f"{wms_med:.6f}",
                    "overhead_s":     f"{ov:.6f}",
                })
                alt = alt_ov.get((system, pat, wl, ch))
                if alt is not None:
                    diffs.append((ebene, system, pat, wl, ch, alt, ov, ov-alt))
        felder = list(zeilen_neu[0].keys())
        with (zielordner / zieldatei).open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=felder)
            w.writeheader(); w.writerows(zeilen_neu)
        # Koordinationsdateien unveraendert uebernehmen
        if ebene != "aiida_konfig":
            for k in (ALT / ebene).glob("*coordination*"):
                shutil.copy(k, zielordner / k.name)
        else:  # Konfigurationsstudie: Koordination aus den Quellordnern
            for system, rel in systeme.items():
                for k in (MESS / rel).glob("*coordination*"):
                    shutil.copy(k, zielordner / f"{system}_coordination_results.csv")
        print(f"[{ebene}] {len(zeilen_neu)} Konfigurationen -> {zielordner/zieldatei}")

    print("\nGroesste Abweichungen alt -> neu (Overhead, Sekunden):")
    for e, s, p, wl, ch, a, n, d in sorted(diffs, key=lambda x: -abs(x[7]))[:12]:
        print(f"  {e:9s} {s:11s} {p[:8]:8s} {wl:6s} c{ch}: {a:8.2f} -> {n:8.2f}  ({d:+.3f})")
    betrag = [abs(d[7]) for d in diffs]
    print(f"  {len(diffs)} Vergleiche, max {max(betrag):.3f} s, Median {st.median(betrag):.3f} s")

if __name__ == "__main__":
    hauptlauf()
