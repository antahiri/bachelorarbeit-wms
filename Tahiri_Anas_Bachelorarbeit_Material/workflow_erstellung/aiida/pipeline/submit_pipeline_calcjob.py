#!/usr/bin/env python3
"""Beobachtungslauf: Pipeline als CalcJobs auf dem lokalen Rechner."""

import sys
from pathlib import Path

from aiida import load_profile
from aiida.engine import submit
from aiida.orm import SinglefileData, Str, load_code

if len(sys.argv) != 2 or sys.argv[1] not in {"short", "medium", "long"}:
    raise SystemExit("Verwendung: python3 submit_pipeline_calcjob.py {short|medium|long}")

workload = sys.argv[1]

load_profile("aiida_pipeline")  # verbindet das Skript mit Datenbank, Repository und Broker

from pipeline_calcjob_workchain import PipelineCalcJobWorkChain  # davor muss PYTHONPATH auf das Verzeichnis zeigen

SCRIPTS = Path.home() / "nextflow_pipeline_test" / "benchmark_scripts"

def node(name, filename=None):
    """Datei als SinglefileData-Knoten in die Datenbank legen."""
    return SinglefileData(file=(SCRIPTS / name).resolve(), filename=filename or name)

builder = PipelineCalcJobWorkChain.get_builder()  # erstellt einen Builder fuer die WorkChain
builder.code = load_code("aiida_python312@localhost_aiida")
builder.workload = Str(workload)
builder.scripts = {  # Skripte als SinglefileData-Knoten in der Datenbank ablegen
    "generate_input":   node("generate_input.py"),
    "preprocess":       node("preprocess.py"),
    "compute_short":    node("compute.py"),
    "compute_medium":   node("compute_medium.py"),
    "compute_long":     node("compute_long.py"),
    "postprocess":      node("postprocess.py"),
    "benchmark_timing": node("benchmark_timing.py"),
}

wc = submit(builder)  # erzeugt WorkChain-Knoten und legt eine Nachricht in RabbitMQ
print(f"WorkChain submitted: PK={wc.pk}")
print(f"Beobachten mit:  verdi -p aiida_pipeline process list -a")
print(f"Bericht:         verdi -p aiida_pipeline process report {wc.pk}")