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

BASE_DIR = Path.home() / "wms_benchmark_reference"

PIPELINE_SOURCE = Path.home() / "nextflow_pipeline_test"
SCATTER_SOURCE = Path.home() / "nextflow_scatter_gather_test"

RESULT_ROOT = BASE_DIR / "results" / "nextflow_task_timing"
RUN_ROOT = BASE_DIR / "run_data" / "nextflow" / "task_timing_benchmark"

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


def run_command(command, cwd: Path) -> None:
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Befehl fehlgeschlagen:\n"
            f"{' '.join(map(str, command))}\n\n"
            f"{result.stderr}"
        )


def read_key_value_file(path: Path) -> dict[str, str]:
    values = {}

    for line in path.read_text().splitlines():
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    return values


def numeric_task_sort_key(task_name: str):
    task_order = {
        "generate_input": (0, 0),
        "preprocess": (1, 0),
        "split": (2, 0),
        "aggregate": (4, 0),
        "postprocess": (5, 0),
    }

    if task_name.startswith("compute_"):
        suffix = task_name.removeprefix("compute_")
        return (3, int(suffix) if suffix.isdigit() else 999)

    return task_order.get(task_name, (99, 0))


def read_task_timings(search_root: Path, expected_compute_tasks: int) -> list[dict]:
    timing_files = list(search_root.rglob("timing_*.txt"))

    if not timing_files:
        raise RuntimeError(f"Keine Timing-Dateien gefunden in: {search_root}")

    timings = []

    for timing_file in timing_files:
        values = read_key_value_file(timing_file)

        required = {"task_name", "task_start_ns", "task_end_ns"}

        if not required.issubset(values):
            raise RuntimeError(
                f"Ungültige Timing-Datei: {timing_file}\n"
                f"Gefunden: {values}"
            )

        timings.append(
            {
                "task_name": values["task_name"],
                "task_start_ns": int(values["task_start_ns"]),
                "task_end_ns": int(values["task_end_ns"]),
                "timing_file": str(timing_file),
            }
        )

    task_names = [row["task_name"] for row in timings]

    duplicates = {
        task_name
        for task_name in task_names
        if task_names.count(task_name) > 1
    }

    if duplicates:
        raise RuntimeError(
            f"Doppelte Timing-Daten gefunden: {sorted(duplicates)}"
        )

    expected = {
        "generate_input",
        "preprocess",
        "postprocess",
    }

    if expected_compute_tasks > 0:
        expected.add("split")
        expected.add("aggregate")
        expected.update(
            f"compute_{index}"
            for index in range(1, expected_compute_tasks + 1)
        )
    else:
        expected.add("compute_1")

    missing = expected.difference(task_names)

    if missing:
        raise RuntimeError(
            f"Fehlende Timing-Dateien für Tasks: {sorted(missing)}"
        )

    return sorted(timings, key=lambda row: numeric_task_sort_key(row["task_name"]))


def seconds(start_ns: int, end_ns: int) -> float:
    return (end_ns - start_ns) / 1_000_000_000


def compute_max_concurrency(compute_rows: list[dict]) -> int:
    events = []

    for row in compute_rows:
        events.append((row["task_start_ns"], 1))
        events.append((row["task_end_ns"], -1))

    # Bei gleichem Zeitpunkt zuerst Ende, dann Start.
    events.sort(key=lambda event: (event[0], event[1]))

    current = 0
    maximum = 0

    for _, delta in events:
        current += delta
        maximum = max(maximum, current)

    return maximum


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


def copy_project(source: Path, destination: Path, workload: str) -> Path:
    if destination.exists():
        shutil.rmtree(destination)

    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(
            "work",
            ".nextflow",
            ".nextflow.log",
            "benchmark_scripts_before_timing",
            "__pycache__",
        ),
    )

    scripts_dir = destination / "benchmark_scripts"
    selected_compute = scripts_dir / WORKLOADS[workload]["compute_file"]
    active_compute = scripts_dir / "compute.py"

    if not selected_compute.exists():
        raise RuntimeError(
            f"Compute-Skript fehlt: {selected_compute}"
        )

    if selected_compute != active_compute:
        shutil.copy2(selected_compute, active_compute)

    return destination


def save_task_table(
    timings: list[dict],
    output_file: Path,
    system: str,
    pattern: str,
    workload: str,
    chunks: int,
    repetition: int,
    run_kind: str,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    rows = []

    for row in timings:
        rows.append(
            {
                "system": system,
                "workflow_pattern": pattern,
                "workload": workload,
                "chunks": chunks,
                "repetition": repetition,
                "run_kind": run_kind,
                "task_name": row["task_name"],
                "task_start_ns": row["task_start_ns"],
                "task_end_ns": row["task_end_ns"],
                "task_duration_seconds": (
                    row["task_end_ns"] - row["task_start_ns"]
                ) / 1_000_000_000,
            }
        )

    with output_file.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


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


def median_or_nan(rows: list[dict], field: str) -> float:
    values = [
        float(row[field])
        for row in rows
        if row.get(field) not in ("", None)
    ]

    return median(values) if values else math.nan


def write_csv(rows: list[dict], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        return

    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)

    with output_file.open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def benchmark_configuration(
    pattern: str,
    workload: str,
    chunks: int,
) -> tuple[list[dict], list[dict]]:
    source = (
        PIPELINE_SOURCE
        if pattern == "pipeline"
        else SCATTER_SOURCE
    )

    repetitions = WORKLOADS[workload]["repetitions"]

    raw_rows = []
    summary_rows = []

    for repetition in range(1, repetitions + 1):
        configuration_root = (
            RUN_ROOT
            / pattern
            / workload
            / f"chunks_{chunks}"
            / f"rep_{repetition}"
        )

        project_dir = copy_project(
            source,
            configuration_root / "project",
            workload,
        )

        reference_dir = configuration_root / "reference"
        nextflow_work_dir = configuration_root / "nextflow_work"

        if pattern == "pipeline":
            reference_timings, reference_outer_runtime = run_pipeline_reference(
                project_dir,
                reference_dir,
            )
        else:
            reference_timings, reference_outer_runtime = run_scatter_reference(
                project_dir,
                reference_dir,
                chunks,
            )

        nextflow_timings, nextflow_outer_runtime = run_nextflow_workflow(
            project_dir,
            nextflow_work_dir,
            pattern,
            chunks,
        )

        reference_metrics = calculate_metrics(
            reference_timings,
            pattern,
        )

        nextflow_metrics = calculate_metrics(
            nextflow_timings,
            pattern,
        )

        overhead = nextflow_outer_runtime - reference_outer_runtime

        task_output_root = (
            RESULT_ROOT
            / "task_timings"
            / pattern
            / workload
            / f"chunks_{chunks}"
            / f"rep_{repetition}"
        )

        save_task_table(
            reference_timings,
            task_output_root / "reference_task_timings.csv",
            "reference",
            pattern,
            workload,
            chunks,
            repetition,
            "reference",
        )

        save_task_table(
            nextflow_timings,
            task_output_root / "nextflow_task_timings.csv",
            "nextflow",
            pattern,
            workload,
            chunks,
            repetition,
            "nextflow",
        )

        row = {
            "system": "nextflow",
            "workflow_pattern": pattern,
            "workload": workload,
            "chunks": chunks,
            "repetition": repetition,
            "reference_outer_runtime_seconds": f"{reference_outer_runtime:.9f}",
            "nextflow_outer_runtime_seconds": f"{nextflow_outer_runtime:.9f}",
            "workflow_execution_overhead_seconds": f"{overhead:.9f}",
            "workflow_execution_overhead_percent": (
                f"{(overhead / reference_outer_runtime) * 100:.6f}"
            ),
            "reference_task_span_seconds": (
                f"{reference_metrics['execution_makespan_seconds']:.9f}"
            ),
            "nextflow_task_span_seconds": (
                f"{nextflow_metrics['execution_makespan_seconds']:.9f}"
            ),
            "reference_compute_phase_seconds": (
                f"{reference_metrics['compute_phase_seconds']:.9f}"
            ),
            "nextflow_compute_phase_seconds": (
                f"{nextflow_metrics['compute_phase_seconds']:.9f}"
            ),
            "reference_start_spread_seconds": (
                f"{reference_metrics['fanout_start_spread_seconds']:.9f}"
            ),
            "nextflow_start_spread_seconds": (
                f"{nextflow_metrics['fanout_start_spread_seconds']:.9f}"
            ),
            "reference_max_concurrent_compute_tasks": (
                reference_metrics["max_concurrent_compute_tasks"]
            ),
            "nextflow_max_concurrent_compute_tasks": (
                nextflow_metrics["max_concurrent_compute_tasks"]
            ),
        }

        for metric_name, metric_value in nextflow_metrics.items():
            if metric_name in {
                "execution_makespan_seconds",
                "compute_phase_seconds",
                "fanout_start_spread_seconds",
                "max_concurrent_compute_tasks",
            }:
                continue

            row[f"nextflow_{metric_name}"] = f"{metric_value:.9f}"
            row[f"reference_{metric_name}"] = (
                f"{reference_metrics[metric_name]:.9f}"
            )

        raw_rows.append(row)

        print(
            f"{pattern} | {workload} | chunks={chunks} | "
            f"Wiederholung {repetition}/{repetitions}: "
            f"Nextflow={nextflow_outer_runtime:.6f} s | "
            f"Referenz={reference_outer_runtime:.6f} s | "
            f"Overhead={overhead:.6f} s"
        )

    summary = {
        "system": "nextflow",
        "workflow_pattern": pattern,
        "workload": workload,
        "chunks": chunks,
    }

    for field in raw_rows[0]:
        if field in {"system", "workflow_pattern", "workload", "chunks", "repetition"}:
            continue

        try:
            summary[f"{field}_median"] = f"{median_or_nan(raw_rows, field):.9f}"
        except ValueError:
            pass

    summary_rows.append(summary)

    return raw_rows, summary_rows


def main() -> None:
    RESULT_ROOT.mkdir(parents=True, exist_ok=True)
    RUN_ROOT.mkdir(parents=True, exist_ok=True)

    all_raw_rows = []
    all_summary_rows = []

    print("\n=== Nextflow Task-Timing-Benchmark ===\n")

    for workload in WORKLOADS:
        print(f"\n--- Pipeline | {workload} ---")

        raw_rows, summary_rows = benchmark_configuration(
            pattern="pipeline",
            workload=workload,
            chunks=1,
        )

        all_raw_rows.extend(raw_rows)
        all_summary_rows.extend(summary_rows)

    for workload in WORKLOADS:
        for chunks in CHUNK_COUNTS:
            print(
                f"\n--- Scatter-Gather | {workload} | "
                f"{chunks} Chunk(s) ---"
            )

            raw_rows, summary_rows = benchmark_configuration(
                pattern="scatter_gather",
                workload=workload,
                chunks=chunks,
            )

            all_raw_rows.extend(raw_rows)
            all_summary_rows.extend(summary_rows)

    write_csv(
        all_raw_rows,
        RESULT_ROOT / "nextflow_task_timing_raw_results.csv",
    )

    write_csv(
        all_summary_rows,
        RESULT_ROOT / "nextflow_task_timing_summary.csv",
    )

    print("\nFertig.")
    print(
        "Rohdaten: "
        f"{RESULT_ROOT / 'nextflow_task_timing_raw_results.csv'}"
    )
    print(
        "Zusammenfassung: "
        f"{RESULT_ROOT / 'nextflow_task_timing_summary.csv'}"
    )


if __name__ == "__main__":
    main()
