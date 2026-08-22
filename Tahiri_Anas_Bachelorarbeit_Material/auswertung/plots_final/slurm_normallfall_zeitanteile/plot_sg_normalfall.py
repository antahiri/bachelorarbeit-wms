#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

CSV = Path("zerlegung_tasks.csv")
OUT = Path("abb_scatter_gather_taskzerlegung_neu.png")

df = pd.read_csv(CSV)

scatter = (
    df[df["pattern"] == "scatter_gather"]
    .groupby("task", as_index=False)[
        ["queue_s", "prolog_env_s", "payload_s"]
    ]
    .median()
)

order = [
    "generate_input",
    "preprocess",
    "split",
    "compute_1",
    "compute_2",
    "compute_3",
    "compute_4",
    "aggregate",
    "postprocess",
]

labels = {
    "generate_input": "Generate Input",
    "preprocess": "Preprocess",
    "split": "Split",
    "compute_1": "Compute 1",
    "compute_2": "Compute 2",
    "compute_3": "Compute 3",
    "compute_4": "Compute 4",
    "aggregate": "Aggregate",
    "postprocess": "Postprocess",
}

scatter["task"] = pd.Categorical(
    scatter["task"],
    categories=order,
    ordered=True,
)

scatter = scatter.sort_values("task")

queue = scatter["queue_s"].to_numpy()
before_payload = scatter["prolog_env_s"].to_numpy()
payload = scatter["payload_s"].to_numpy()

names = [labels[x] for x in scatter["task"].astype(str)]

QUEUE_COLOR = "#59636E"
BEFORE_COLOR = "#B84A62"
PAYLOAD_COLOR = "#168C7A"

fig, ax = plt.subplots(
    figsize=(14.26, 10.83),
    dpi=200
)

y = np.arange(len(names))
bar_height = 0.74

# Queue
ax.barh(
    y,
    queue,
    height=bar_height,
    color=QUEUE_COLOR,
    label="Queue-Wartezeit",
)

# Zeit vor Nutzlast
ax.barh(
    y,
    before_payload,
    left=queue,
    height=bar_height,
    color=BEFORE_COLOR,
    label="Zeit vor Nutzlast",
)

# Payload
ax.barh(
    y,
    payload,
    left=queue + before_payload,
    height=bar_height,
    color=PAYLOAD_COLOR,
    label="Payload",
)

# Beschriftungen innerhalb der Balken
for i in range(len(y)):

    ax.text(
        queue[i] / 2,
        y[i],
        f"{queue[i]:.0f} s",
        ha="center",
        va="center",
        color="white",
        fontsize=16,
        fontweight="bold",
    )

    ax.text(
        queue[i] + before_payload[i] / 2,
        y[i],
        f"{before_payload[i]:.1f} s".replace(".", ","),
        ha="center",
        va="center",
        color="white",
        fontsize=16,
        fontweight="bold",
    )


def payload_text(v):
    if v < 0.1:
        return f"{v:.3f}".replace(".", ",")
    return f"{v:.1f}".replace(".", ",")


# Payload-Beschriftungen rechts
for i in range(len(y)):

    end = queue[i] + before_payload[i] + payload[i]

    ax.plot(
        [end, end + 3.5],
        [y[i], y[i]],
        color=PAYLOAD_COLOR,
        linewidth=1.5,
    )

    ax.text(
        end + 3.8,
        y[i],
        f"Payload {payload_text(payload[i])} s",
        va="center",
        ha="left",
        color=PAYLOAD_COLOR,
        fontsize=15,
        fontweight="bold",
    )

# Achsen
ax.set_yticks(y)
ax.set_yticklabels(names, fontsize=16)

ax.invert_yaxis()

# Mehr Platz rechts für die Payload-Texte
ax.set_xlim(0, 84)
ax.set_xticks(np.arange(0, 81, 10))
ax.tick_params(axis="x", labelsize=14)

ax.set_xlabel(
    "Zeit von Submit bis Ende der Payload (s)",
    fontsize=16,
)

ax.xaxis.grid(
    True,
    color="#D9DEE3",
    linewidth=1,
)

ax.set_axisbelow(True)

# Titel
ax.set_title(
    "Scatter-Gather (short, 4 Chunks)",
    loc="left",
    fontsize=20,
    fontweight="bold",
    pad=40,
)

# Untertitel
ax.text(
    0,
    1.025,
    "Median je Slurm-Job über fünf Wiederholungen",
    transform=ax.transAxes,
    fontsize=16,
    color="#59616B",
    va="bottom",
)

# Rahmen
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

ax.spines["left"].set_linewidth(1.2)
ax.spines["bottom"].set_linewidth(1.2)

# Legende
ax.legend(
    loc="upper center",
    bbox_to_anchor=(0.5, -0.10),
    ncol=3,
    frameon=False,
    fontsize=14,
    handlelength=1.4,
    columnspacing=2.2,
)

# Abstände
plt.subplots_adjust(
    left=0.14,
    right=0.94,
    top=0.87,
    bottom=0.15,
)

# Export
plt.savefig(
    OUT,
    dpi=200,
    bbox_inches=None,
    facecolor="white",
)

plt.close()

print(f"Gespeichert: {OUT.resolve()}")