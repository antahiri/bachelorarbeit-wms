#!/usr/bin/env python3
""" Scatter-Gather als CalcJobs auf dem lokalen Rechner."""

import sys
from pathlib import Path

from aiida import load_profile
from aiida.engine import submit
from aiida.orm import Int, SinglefileData, Str, load_code

if len(sys.argv) != 3:
    raise SystemExit("Verwendung: python3 submit_scatter_gather_calcjob.py {short|medium|long} {1|2|4}")

workload = sys.argv[1]
chunks = int(sys.argv[2])

if workload not in {"short", "medium", "long"}:
    raise SystemExit("Workload muss short, medium oder long sein.")

if chunks not in {1, 2, 4}:
    raise SystemExit("Chunks muessen 1, 2 oder 4 sein.") # (kann auch mehr sein)

load_profile("aiida_pipeline")  # verbindet das Skript mit Datenbank, Repository und Broker

from scatter_gather_calcjob_workchain import ScatterGatherCalcJobWorkChain  # davor muss PYTHONPATH auf das Verzeichnis zeigen

SCRIPTS = Path.home() / "nextflow_scatter_gather_test" / "benchmark_scripts"

def node(name, filename=None):
    """Datei als SinglefileData-Knoten in die Datenbank legen."""
    return SinglefileData(file=(SCRIPTS / name).resolve(), filename=filename or name)

builder = ScatterGatherCalcJobWorkChain.get_builder()  # erstellt einen Builder fuer die WorkChain
builder.code = load_code("aiida_python312@localhost_aiida")
builder.workload = Str(workload)
builder.num_chunks = Int(chunks)
builder.scripts = {  # Skripte als SinglefileData-Knoten in der Datenbank ablegen
    "generate_input":   node("generate_input.py"),
    "preprocess":       node("preprocess.py"),
    "split":            node("split.py"),
    "compute_short":    node("compute.py"),
    "compute_medium":   node("compute_medium.py"),
    "compute_long":     node("compute_long.py"),
    "aggregate":        node("aggregate.py"),
    "postprocess":      node("postprocess.py"),
    "benchmark_timing": node("benchmark_timing.py"),
}

wc = submit(builder)  # erzeugt WorkChain-Knoten und legt eine Nachricht in RabbitMQ
print(f"WorkChain submitted: PK={wc.pk} ({workload}, {chunks} Chunks)")
print(f"Beobachten mit:  verdi -p aiida_pipeline process list -a")
print(f"Bericht:         verdi -p aiida_pipeline process report {wc.pk}")
