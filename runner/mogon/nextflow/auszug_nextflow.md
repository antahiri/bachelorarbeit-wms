# Auszug aus run_nextflow_mogon.py

## Messparameter (Zeilen 1-40)
```python
#!/usr/bin/env python3

import csv
import math
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median

BASE_DIR = Path.home() / "wms_hpc_benchmark"

PIPELINE_SOURCE = BASE_DIR / "nextflow" / "pipeline"
SCATTER_SOURCE = BASE_DIR / "nextflow" / "scatter_gather"

RESULT_ROOT = BASE_DIR / "results" / "nextflow"
RUN_ROOT = BASE_DIR / "run_data" / "nextflow"

PYTHON = sys.executable
NEXTFLOW = "nextflow"

WORKLOADS = {
    "short": {
        "compute_file": "compute.py",
        "repetitions": 5,
    },
    "medium": {
        "compute_file": "compute_medium.py",
        "repetitions": 5,
    },
    "long": {
        "compute_file": "compute_long.py",
        "repetitions": 3,
    },
}

CHUNK_COUNTS = [1, 2, 4]

```

## Referenzmessung (Zeilen 333-464)
```python
def run_pipeline_reference(project_dir: Path, run_dir: Path) -> tuple[list[dict], float]:
    run_dir.mkdir(parents=True, exist_ok=True)

    scripts_dir = project_dir / "benchmark_scripts"
    outer_start = time.perf_counter()

    run_command(
        [PYTHON, str(scripts_dir / "generate_input.py"), "raw_input.txt"],
        run_dir,
    )

    run_command(
        [
            PYTHON,
            str(scripts_dir / "preprocess.py"),
            "raw_input.txt",
            "prepared_input.txt",
        ],
        run_dir,
    )

    run_command(
        [
            PYTHON,
            str(scripts_dir / "compute.py"),
            "prepared_input.txt",
            "result.txt",
        ],
        run_dir,
    )

    run_command(
        [
            PYTHON,
            str(scripts_dir / "postprocess.py"),
            "result.txt",
            "summary.txt",
        ],
        run_dir,
    )

    outer_runtime_seconds = time.perf_counter() - outer_start

    return (
        read_task_timings(run_dir, expected_compute_tasks=0),
        outer_runtime_seconds,
    )


def run_scatter_reference(
    project_dir: Path,
    run_dir: Path,
    chunks: int,
) -> tuple[list[dict], float]:
    run_dir.mkdir(parents=True, exist_ok=True)

    scripts_dir = project_dir / "benchmark_scripts"
    outer_start = time.perf_counter()

    run_command(
        [
            PYTHON,
            str(scripts_dir / "generate_input.py"),
            "raw_input.txt",
        ],
        run_dir,
    )

    run_command(
        [
            PYTHON,
            str(scripts_dir / "preprocess.py"),
            "raw_input.txt",
            "prepared_input.txt",
        ],
        run_dir,
    )

    run_command(
        [
            PYTHON,
            str(scripts_dir / "split.py"),
            "prepared_input.txt",
            str(chunks),
        ],
        run_dir,
    )

    def compute_task(index: int) -> None:
        run_command(
            [
                PYTHON,
                str(scripts_dir / "compute.py"),
                f"chunk_{index}.txt",
                f"result_{index}.txt",
            ],
            run_dir,
        )

    with ThreadPoolExecutor(max_workers=chunks) as executor:
        list(executor.map(compute_task, range(1, chunks + 1)))

    result_files = [f"result_{index}.txt" for index in range(1, chunks + 1)]

    run_command(
        [
            PYTHON,
            str(scripts_dir / "aggregate.py"),
            *result_files,
            "aggregated_result.txt",
        ],
        run_dir,
    )

    run_command(
        [
            PYTHON,
            str(scripts_dir / "postprocess.py"),
            "aggregated_result.txt",
            "summary.txt",
        ],
        run_dir,
    )

    outer_runtime_seconds = time.perf_counter() - outer_start

    return (
        read_task_timings(run_dir, expected_compute_tasks=chunks),
        outer_runtime_seconds,
    )


```

## Systemmessung (Zeilen 465-514)
```python
def run_nextflow_workflow(
    project_dir: Path,
    work_dir: Path,
    pattern: str,
    chunks: int,
) -> tuple[list[dict], float]:
    command = [
        NEXTFLOW,
        "run",
        "benchmark_main.nf",
        "-ansi-log",
        "false",
        "-work-dir",
        str(work_dir),
    ]

    if pattern == "scatter_gather":
        command.extend(["--chunks", str(chunks)])

    outer_start = time.perf_counter()
    run_command(command, project_dir)
    outer_runtime_seconds = time.perf_counter() - outer_start

    expected_compute_tasks = chunks if pattern == "scatter_gather" else 0

    timings = read_task_timings(work_dir, expected_compute_tasks)

    if pattern == "pipeline":
        compute_rows = [
            row for row in timings
            if row["task_name"] == "compute"
        ]

        if len(compute_rows) == 1:
            compute_row = compute_rows[0]
            old_file = Path(compute_row["timing_file"])
            new_file = old_file.with_name("timing_compute_1.txt")

            new_file.write_text(
                f"task_name=compute_1\n"
                f"task_start_ns={compute_row['task_start_ns']}\n"
                f"task_end_ns={compute_row['task_end_ns']}\n"
            )

            old_file.unlink()
            timings = read_task_timings(work_dir, expected_compute_tasks=0)

    return timings, outer_runtime_seconds


```

## Berechnung der Koordinationsmetriken (Zeilen 179-254)
```python
def calculate_metrics(timings: list[dict], pattern: str) -> dict[str, float]:
    by_name = {row["task_name"]: row for row in timings}

    first_start = min(row["task_start_ns"] for row in timings)
    last_end = max(row["task_end_ns"] for row in timings)

    metrics = {
        "execution_makespan_seconds": seconds(first_start, last_end),
        "generate_to_preprocess_latency_seconds": seconds(
            by_name["generate_input"]["task_end_ns"],
            by_name["preprocess"]["task_start_ns"],
        ),
    }

    if pattern == "pipeline":
        metrics.update(
            {
                "preprocess_to_compute_latency_seconds": seconds(
                    by_name["preprocess"]["task_end_ns"],
                    by_name["compute_1"]["task_start_ns"],
                ),
                "compute_to_postprocess_latency_seconds": seconds(
                    by_name["compute_1"]["task_end_ns"],
                    by_name["postprocess"]["task_start_ns"],
                ),
                "fanout_start_spread_seconds": 0.0,
                "compute_phase_seconds": seconds(
                    by_name["compute_1"]["task_start_ns"],
                    by_name["compute_1"]["task_end_ns"],
                ),
                "max_concurrent_compute_tasks": 1,
            }
        )
        return metrics

    compute_rows = [
        row for row in timings if row["task_name"].startswith("compute_")
    ]

    first_compute_start = min(row["task_start_ns"] for row in compute_rows)
    last_compute_end = max(row["task_end_ns"] for row in compute_rows)
    last_compute_finish = max(row["task_end_ns"] for row in compute_rows)

    metrics.update(
        {
            "preprocess_to_split_latency_seconds": seconds(
                by_name["preprocess"]["task_end_ns"],
                by_name["split"]["task_start_ns"],
            ),
            "split_to_first_compute_latency_seconds": seconds(
                by_name["split"]["task_end_ns"],
                first_compute_start,
            ),
            "fanout_start_spread_seconds": seconds(
                first_compute_start,
                max(row["task_start_ns"] for row in compute_rows),
            ),
            "compute_phase_seconds": seconds(
                first_compute_start,
                last_compute_end,
            ),
            "last_compute_to_aggregate_latency_seconds": seconds(
                last_compute_finish,
                by_name["aggregate"]["task_start_ns"],
            ),
            "aggregate_to_postprocess_latency_seconds": seconds(
                by_name["aggregate"]["task_end_ns"],
                by_name["postprocess"]["task_start_ns"],
            ),
            "max_concurrent_compute_tasks": compute_max_concurrency(compute_rows),
        }
    )

    return metrics


```
