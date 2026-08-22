import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_central, speichere, dezimal,
                    NAME, FARBE, GROESSE_HALB)
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba

stil()
exc = lade_central("mogon_exc")
sha = lade_central("mogon_sha")
SYS = ["streamflow", "nextflow", "merlin", "aiida"]

daten = []
for s in SYS:
    d = sorted(exc[k]["wms_makespan_s"] - sha[k]["wms_makespan_s"]
               for k in exc if k[0] == s)
    assert len(d) == 12, f"{s}: {len(d)} statt 12 Konfigurationen"
    daten.append(d)

fig, ax = plt.subplots(figsize=(GROESSE_HALB[0] + 0.9, GROESSE_HALB[1]))
ax.axhline(0, color="black", lw=0.9, ls="--", zorder=1)
for i, (s, d) in enumerate(zip(SYS, daten)):
    ax.boxplot([d], positions=[i * 1.5], widths=0.78, whis=1.5,
               showfliers=False, patch_artist=True, zorder=2,
               boxprops=dict(facecolor=to_rgba(FARBE[s], 0.12),
                             edgecolor=FARBE[s], lw=1.0),
               medianprops=dict(color=FARBE[s], lw=1.6),
               whiskerprops=dict(color=FARBE[s], lw=0.9),
               capprops=dict(color=FARBE[s], lw=0.9))
    ax.scatter([i * 1.5] * len(d), d, color=FARBE[s], alpha=0.55, s=30,
               edgecolors="none", zorder=3)
    import statistics
    m = statistics.median(d)
    ax.text(i * 1.5 + 0.46, m, f"{m:+.3f} s".replace(".", ","),
            fontsize=8.5, va="center", color=FARBE[s])

ax.set_xlim(-0.5, (len(SYS) - 1) * 1.5 + 1.30)
ax.set_xticks([i * 1.5 for i in range(len(SYS))])
ax.set_xticklabels([NAME[s] for s in SYS])
ax.set_ylabel("Differenz der Gesamtlaufzeit in s\n(exklusiv minus geteilt)")
dezimal(ax, vorzeichen=True)

speichere(fig, "mogon", "05_delta_exklusiv_geteilt_mogon.png")
