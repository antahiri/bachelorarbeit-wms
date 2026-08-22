#!/usr/bin/env python3
"""Beobachtungslauf: Scatter-Gather short c4 als CalcJobs ueber Slurm."""

from pathlib import Path

from aiida import load_profile
from aiida.engine import submit
from aiida.orm import Int, SinglefileData, Str, load_code

load_profile("slurm_modus")

from scatter_gather_calcjob_workchain import ScatterGatherCalcJobWorkChain

SCRIPTS = Path.home() / "wms_hpc_benchmark" / "benchmark_scripts" / "scatter_gather"

def node(name, filename=None):
    return SinglefileData(file=SCRIPTS / name, filename=filename or name)

builder = ScatterGatherCalcJobWorkChain.get_builder()
builder.code = load_code("aiida_python312@mogon-slurm")
builder.workload = Str("short")
builder.num_chunks = Int(4)
builder.scripts = {
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

wc = submit(builder)
print(f"WorkChain submitted: PK={wc.pk}")
