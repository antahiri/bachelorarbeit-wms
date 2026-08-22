#!/usr/bin/env python3

from pathlib import Path
from Pegasus.api import *

base = Path(".").resolve()
scripts = base / "benchmark_scripts"

tc = TransformationCatalog()

generate_t = Transformation(
    "generate",
    site="condorpool",
    pfn=(scripts / "generate_input.py").resolve().as_uri(),
    is_stageable=False,
    arch=Arch.X86_64,
    os_type=OS.MACOSX,
)

preprocess_t = Transformation(
    "preprocess",
    site="condorpool",
    pfn=(scripts / "preprocess.py").resolve().as_uri(),
    is_stageable=False,
    arch=Arch.X86_64,
    os_type=OS.MACOSX,
)

compute_t = Transformation(
    "compute",
    site="condorpool",
    pfn=(scripts / "compute.py").resolve().as_uri(),
    is_stageable=False,
    arch=Arch.X86_64,
    os_type=OS.MACOSX,
)

postprocess_t = Transformation(
    "postprocess",
    site="condorpool",
    pfn=(scripts / "postprocess.py").resolve().as_uri(),
    is_stageable=False,
    arch=Arch.X86_64,
    os_type=OS.MACOSX,
)

tc.add_transformations(
    generate_t,
    preprocess_t,
    compute_t,
    postprocess_t,
)

tc.write("transformations.yml")

workflow = Workflow("pipeline-benchmark")

raw_file = File("raw_input.txt")
prepared_file = File("prepared_input.txt")
result_file = File("result.txt")
summary_file = File("summary.txt")

generate_job = Job("generate")
generate_job.add_args("raw_input.txt")
generate_job.add_outputs(
    raw_file,
    stage_out=False,
    register_replica=False,
)

preprocess_job = Job("preprocess")
preprocess_job.add_args("raw_input.txt", "prepared_input.txt")
preprocess_job.add_inputs(raw_file)
preprocess_job.add_outputs(
    prepared_file,
    stage_out=False,
    register_replica=False,
)

compute_job = Job("compute")
compute_job.add_args("prepared_input.txt", "result.txt")
compute_job.add_inputs(prepared_file)
compute_job.add_outputs(
    result_file,
    stage_out=False,
    register_replica=False,
)

postprocess_job = Job("postprocess")
postprocess_job.add_args("result.txt", "summary.txt")
postprocess_job.add_inputs(result_file)
postprocess_job.add_outputs(
    summary_file,
    stage_out=True,
    register_replica=True,
)

workflow.add_jobs(
    generate_job,
    preprocess_job,
    compute_job,
    postprocess_job,
)

workflow.write("workflow.yml")
