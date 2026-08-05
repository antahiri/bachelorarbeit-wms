# Auszug aus run_streamflow_mogon.py

## Messparameter (Zeilen 1-40)
```python
#!/usr/bin/env python3

import argparse
import csv
import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median

BASE_DIR = Path.home() / "wms_hpc_benchmark"
PIPELINE_SOURCE = BASE_DIR / "benchmark_scripts" / "pipeline"
SCATTER_SOURCE = BASE_DIR / "benchmark_scripts" / "scatter_gather"
PIPELINE_REFERENCE_SCRIPTS = PIPELINE_SOURCE
SCATTER_REFERENCE_SCRIPTS = SCATTER_SOURCE
RESULT_ROOT = BASE_DIR / "results" / "streamflow"
RUN_ROOT = BASE_DIR / "run_data" / "streamflow"

STREAMFLOW = shutil.which("streamflow") or str(
    Path.home() / ".conda" / "envs" / "streamflow-new" / "bin" / "streamflow"
)
PYTHON = shutil.which("python3")

THREAD_LIMITS = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "VECLIB_MAXIMUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
}

WORKLOADS = {
    "short": {"compute_file": "compute.py", "repetitions": 5},
    "medium": {"compute_file": "compute_medium.py", "repetitions": 5},
    "long": {"compute_file": "compute_long.py", "repetitions": 3},
}
CHUNK_COUNTS = (1, 2, 4)

```

## Referenz- und Systemmessung (Zeilen 722-825)
```python
def run_reference_task(
    scripts: Path,
    output: Path,
    script_name: str,
    *arguments: str,
) -> None:
    """Führt einen Referenz-Task mit demselben lokalen Python aus."""
    run_command(
        [PYTHON, str(scripts / script_name), *arguments],
        output,
    )


def run_pipeline_reference(project: Path, output: Path) -> float:
    scripts = project / "scripts"
    output.mkdir(parents=True)
    start = time.perf_counter()
    run_reference_task(scripts, output, "generate_input.py", "raw_input.txt")
    run_reference_task(
        scripts,
        output,
        "preprocess.py",
        "raw_input.txt",
        "prepared_input.txt",
    )
    run_reference_task(
        scripts,
        output,
        "compute.py",
        "prepared_input.txt",
        "result.txt",
    )
    run_reference_task(
        scripts,
        output,
        "postprocess.py",
        "result.txt",
        "summary.txt",
    )
    return time.perf_counter() - start


def run_scatter_reference(project: Path, output: Path, chunks: int) -> float:
    scripts = project / "scripts"
    output.mkdir(parents=True)
    start = time.perf_counter()
    run_reference_task(scripts, output, "generate_input.py", "raw_input.txt")
    run_reference_task(
        scripts,
        output,
        "preprocess.py",
        "raw_input.txt",
        "prepared_input.txt",
    )
    run_reference_task(
        scripts,
        output,
        "split.py",
        "prepared_input.txt",
        str(chunks),
    )

    def compute(index: int) -> None:
        run_reference_task(
            scripts,
            output,
            "compute.py",
            f"chunk_{index}.txt",
            f"result_{index}.txt",
        )

    with ThreadPoolExecutor(max_workers=chunks) as executor:
        list(executor.map(compute, range(1, chunks + 1)))
    run_reference_task(
        scripts,
        output,
        "aggregate.py",
        *(f"result_{index}.txt" for index in range(1, chunks + 1)),
        "aggregated_result.txt",
    )
    run_reference_task(
        scripts,
        output,
        "postprocess.py",
        "aggregated_result.txt",
        "summary.txt",
    )
    return time.perf_counter() - start


def run_streamflow(project: Path, output: Path) -> float:
    output.mkdir(parents=True)
    start = time.perf_counter()
    run_command(
        [
            STREAMFLOW,
            "run",
            "--quiet",
            "--outdir",
            str(output),
            "streamflow.yml",
        ],
        project,
    )
```

## Berechnung der Koordinationsmetriken (Zeilen 641-693)
```python
def calculate_metrics(timings: list[dict], pattern: str) -> dict[str, float | str]:
    by_name = {row["task_name"]: row for row in timings}
    compute_rows = [
        row for row in timings if row["task_name"].startswith("compute_")
    ]
    first_compute_start = min(row["task_start_ns"] for row in compute_rows)
    last_compute_start = max(row["task_start_ns"] for row in compute_rows)
    last_compute_end = max(row["task_end_ns"] for row in compute_rows)
    metrics: dict[str, float | str] = {
        "task_span_s": seconds(
            min(row["task_start_ns"] for row in timings),
            max(row["task_end_ns"] for row in timings),
        ),
        "gen_to_pre_s": seconds(
            by_name["generate_input"]["task_end_ns"],
            by_name["preprocess"]["task_start_ns"],
        ),
        "compute_phase_s": seconds(first_compute_start, last_compute_end),
        "start_spread_s": seconds(first_compute_start, last_compute_start),
        "pre_to_comp_s": "",
        "comp_to_post_s": "",
        "pre_to_split_s": "",
        "split_to_comp_s": "",
        "comp_to_agg_s": "",
        "agg_to_post_s": "",
    }
    if pattern == "pipeline":
        metrics["pre_to_comp_s"] = seconds(
            by_name["preprocess"]["task_end_ns"],
            by_name["compute_1"]["task_start_ns"],
        )
        metrics["comp_to_post_s"] = seconds(
            by_name["compute_1"]["task_end_ns"],
            by_name["postprocess"]["task_start_ns"],
        )
        metrics["compute_phase_s"] = ""
        metrics["start_spread_s"] = ""
    else:
        metrics["pre_to_split_s"] = seconds(
            by_name["preprocess"]["task_end_ns"],
            by_name["split"]["task_start_ns"],
        )
        metrics["split_to_comp_s"] = seconds(
            by_name["split"]["task_end_ns"], first_compute_start
        )
        metrics["comp_to_agg_s"] = seconds(
            last_compute_end, by_name["aggregate"]["task_start_ns"]
        )
        metrics["agg_to_post_s"] = seconds(
            by_name["aggregate"]["task_end_ns"],
            by_name["postprocess"]["task_start_ns"],
        )
    return metrics
```
