import sys
import statistics
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (stil, lade_koordination, speichere, dezimal,
                    NAME, FARBE, GROESSE_DELTA, UEBERGAENGE)
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba

stil()
exc = lade_koordination("mogon_exc")
sha = lade_koordination("mogon_sha")
SYS = ["streamflow", "nextflow", "merlin", "aiida"]

def latenz(quelle, k):
    """Median ueber die Uebergaenge einer Konfiguration."""
    werte = [quelle[k][f] for f in UEBERGAENGE[k[1]]
             if quelle[k].get(f) is not None]
    return statistics.median(werte) if werte else None

daten = []
for s in SYS:
    d = []
    for k in exc:
        if k[0] != s or k not in sha:
            continue
        a, b = latenz(exc, k), latenz(sha, k)
        if a is not None and b is not None:
            d.append(a - b)
    assert len(d) == 12, f"{s}: {len(d)} statt 12 Konfigurationen"
    daten.append(sorted(d))

fig, ax = plt.subplots(figsize=GROESSE_DELTA)
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
    m = statistics.median(d)
    ax.text(i * 1.5 + 0.46, m, f"{m:+.3f} s".replace(".", ","),
            fontsize=8.5, va="center", color=FARBE[s])

ax.set_xlim(-0.5, (len(SYS) - 1) * 1.5 + 1.30)
ax.set_xticks([i * 1.5 for i in range(len(SYS))])
ax.set_xticklabels([NAME[s] for s in SYS])
ax.set_ylabel("Differenz der Übergangslatenz in s\n(exklusiv minus geteilt)")
dezimal(ax, vorzeichen=True)

speichere(fig, "mogon", "07_delta_uebergangslatenzen_mogon.png")
