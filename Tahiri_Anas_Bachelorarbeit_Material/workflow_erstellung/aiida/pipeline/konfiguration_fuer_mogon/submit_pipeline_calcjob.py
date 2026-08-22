#!/usr/bin/env python3
"""Beobachtungslauf: Pipeline short als CalcJobs ueber den Slurm-Scheduler."""

from pathlib import Path

from aiida import load_profile
from aiida.engine import submit
from aiida.orm import SinglefileData, Str, load_code

load_profile("slurm_modus")

from pipeline_calcjob_workchain import PipelineCalcJobWorkChain

SCRIPTS = Path.home() / "wms_hpc_benchmark" / "benchmark_scripts" / "pipeline"

def node(name, filename=None):
    """Datei als SinglefileData-Knoten in die Datenbank legen."""
    return SinglefileData(file=SCRIPTS / name, filename=filename or name)

builder = PipelineCalcJobWorkChain.get_builder()
builder.code = load_code("aiida_python312@mogon-slurm")
builder.workload = Str("short")
builder.scripts = {
    "generate_input":   node("generate_input.py"),
    "preprocess":       node("preprocess.py"),
    "compute_short":    node("compute.py"),
    "compute_medium":   node("compute_medium.py"),
    "compute_long":     node("compute_long.py"),
    "postprocess":      node("postprocess.py"),
    "benchmark_timing": node("benchmark_timing.py"),
}

wc = submit(builder)
print(f"WorkChain submitted: PK={wc.pk}")
print(f"Beobachten mit:  verdi -p slurm_modus process list -a")
print(f"Bericht:         verdi -p slurm_modus process report {wc.pk}")
