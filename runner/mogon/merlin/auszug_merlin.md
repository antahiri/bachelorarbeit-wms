# Auszug aus run_merlin_mogon.py

## Messparameter (Zeilen 1-40)
```python
#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from statistics import median


HOME = Path.home()

BASE_DIR = HOME / "wms_hpc_benchmark"

PIPELINE_ROOT = BASE_DIR / "merlin" / "pipeline"
SCATTER_ROOT = BASE_DIR / "merlin" / "scatter_gather"

RUN_DATA_ROOT = BASE_DIR / "run_data" / "merlin"

RESULTS_ROOT = BASE_DIR / "results" / "merlin"

RAW_FILE = RESULTS_ROOT / "merlin_raw_results.csv"
CENTRAL_FILE = RESULTS_ROOT / "merlin_central_results.csv"
COORDINATION_FILE = RESULTS_ROOT / "merlin_coordination_results.csv"

SYSTEM = "Merlin"

WORKLOAD_REPETITIONS = {
    "short": 5,
    "medium": 5,
    "long": 3,
}

```

## Referenzmessung (Zeilen 236-351)
```python
def direct_reference_pipeline(workspace: Path, workload: str) -> dict[str, dict[str, int]]:
    scripts = PIPELINE_ROOT / "scripts"
    python = sys.executable

    run_command([python, str(scripts / "generate_input.py"), "raw_input.txt"], workspace)

    run_command(
        [
            python,
            str(scripts / "preprocess.py"),
            "raw_input.txt",
            "prepared_input.txt",
        ],
        workspace,
    )

    run_command(
        [
            python,
            str(scripts / COMPUTE_SCRIPTS[workload]),
            "prepared_input.txt",
            "result.txt",
        ],
        workspace,
    )

    run_command(
        [
            python,
            str(scripts / "postprocess.py"),
            "result.txt",
            "summary.txt",
        ],
        workspace,
    )

    return read_reference_timings(workspace, "Pipeline", 1)


def direct_reference_scatter(
    workspace: Path,
    workload: str,
    chunks: int,
) -> dict[str, dict[str, int]]:
    scripts = SCATTER_ROOT / "scripts"
    python = sys.executable

    run_command([python, str(scripts / "generate_input.py"), "raw_input.txt"], workspace)

    run_command(
        [
            python,
            str(scripts / "preprocess.py"),
            "raw_input.txt",
            "prepared_input.txt",
        ],
        workspace,
    )

    run_command(
        [
            python,
            str(scripts / "split.py"),
            "prepared_input.txt",
            str(chunks),
        ],
        workspace,
    )

    processes = []

    for index in range(1, chunks + 1):
        processes.append(
            subprocess.Popen(
                [
                    python,
                    str(scripts / COMPUTE_SCRIPTS[workload]),
                    f"chunk_{index}.txt",
                    f"result_{index}.txt",
                ],
                cwd=workspace,
                env=benchmark_environment(),
            )
        )

    for process in processes:
        return_code = process.wait()

        if return_code != 0:
            raise RuntimeError("Fehler in einem direkten Compute-Referenzprozess.")

    result_files = [f"result_{index}.txt" for index in range(1, chunks + 1)]

    run_command(
        [
            python,
            str(scripts / "aggregate.py"),
            *result_files,
            "aggregated_result.txt",
        ],
        workspace,
    )

    run_command(
        [
            python,
            str(scripts / "postprocess.py"),
            "aggregated_result.txt",
            "summary.txt",
        ],
        workspace,
    )

    return read_reference_timings(workspace, "Scatter-Gather", chunks)


```

## Systemmessung mit Warten auf Fertigstellung (Zeilen 352-427)
```python
def wait_for_merlin_completion(workspace: Path, timeout_seconds: int = 7200) -> None:
    status_file = workspace / "postprocess" / "MERLIN_STATUS.json"
    finished_file = workspace / "postprocess" / "MERLIN_FINISHED"

    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if status_file.exists():
            status_text = status_file.read_text()

            if "FAILED" in status_text or "CANCELLED" in status_text:
                raise RuntimeError(
                    f"Merlin-Workflow fehlgeschlagen:\n{status_text}"
                )

            if finished_file.exists() and "FINISHED" in status_text:
                return

        time.sleep(0.02)

    raise TimeoutError(
        f"Merlin-Workflow wurde nicht innerhalb von {timeout_seconds} Sekunden beendet."
    )


def run_merlin_workflow(
    project_root: Path,
    yaml_name: str,
    destination: Path,
    pattern: str,
    chunks: int,
) -> tuple[float, dict[str, dict[str, int]]]:
    command = ["merlin", "run", yaml_name]

    start_ns = time.time_ns()

    completed = subprocess.run(
        command,
        cwd=project_root,
        env=benchmark_environment(),
        text=True,
        capture_output=True,
    )

    output = completed.stdout + "\n" + completed.stderr

    if completed.returncode != 0:
        print(output)
        raise RuntimeError(f"Merlin-Start fehlgeschlagen: {yaml_name}")

    match = re.search(r"Study workspace is '([^']+)'", output)

    if not match:
        raise RuntimeError(
            "Merlin-Workspace konnte nicht aus der Ausgabe bestimmt werden.\n"
            + output
        )

    workspace = Path(match.group(1))

    wait_for_merlin_completion(workspace)

    end_ns = time.time_ns()

    timings = read_task_timings(workspace, pattern, chunks)

    destination.mkdir(parents=True, exist_ok=True)
    target_workspace = destination / workspace.name

    if target_workspace.exists():
        shutil.rmtree(target_workspace)

    shutil.move(str(workspace), str(target_workspace))

    return seconds(start_ns, end_ns), timings

```

## Berechnung der Koordinationsmetriken (Zeilen 429-513)
```python
def metric_values(
    pattern: str,
    chunks: int,
    timings: dict[str, dict[str, int]],
) -> dict[str, float | str]:
    first_start = min(value["task_start_ns"] for value in timings.values())
    last_end = max(value["task_end_ns"] for value in timings.values())

    metrics: dict[str, float | str] = {
        "wms_task_span_s": seconds(first_start, last_end),
        "wms_gen_to_pre_s": seconds(
            timings["generate_input"]["task_end_ns"],
            timings["preprocess"]["task_start_ns"],
        ),
        "wms_pre_to_comp_s": "",
        "wms_comp_to_post_s": "",
        "wms_pre_to_split_s": "",
        "wms_split_to_comp_s": "",
        "wms_comp_to_agg_s": "",
        "wms_agg_to_post_s": "",
        "wms_start_spread_s": "",
        "wms_compute_phase_s": "",
    }

    if pattern == "Pipeline":
        metrics["wms_pre_to_comp_s"] = seconds(
            timings["preprocess"]["task_end_ns"],
            timings["compute_1"]["task_start_ns"],
        )
        metrics["wms_comp_to_post_s"] = seconds(
            timings["compute_1"]["task_end_ns"],
            timings["postprocess"]["task_start_ns"],
        )
        return metrics

    compute_names = [f"compute_{index}" for index in range(1, chunks + 1)]

    earliest_compute_start = min(
        timings[name]["task_start_ns"]
        for name in compute_names
    )

    latest_compute_start = max(
        timings[name]["task_start_ns"]
        for name in compute_names
    )

    latest_compute_end = max(
        timings[name]["task_end_ns"]
        for name in compute_names
    )

    metrics["wms_pre_to_split_s"] = seconds(
        timings["preprocess"]["task_end_ns"],
        timings["split"]["task_start_ns"],
    )

    metrics["wms_split_to_comp_s"] = seconds(
        timings["split"]["task_end_ns"],
        earliest_compute_start,
    )

    metrics["wms_comp_to_agg_s"] = seconds(
        latest_compute_end,
        timings["aggregate"]["task_start_ns"],
    )

    metrics["wms_agg_to_post_s"] = seconds(
        timings["aggregate"]["task_end_ns"],
        timings["postprocess"]["task_start_ns"],
    )

    metrics["wms_start_spread_s"] = seconds(
        earliest_compute_start,
        latest_compute_start,
    )

    metrics["wms_compute_phase_s"] = seconds(
        earliest_compute_start,
        latest_compute_end,
    )

    return metrics


```
