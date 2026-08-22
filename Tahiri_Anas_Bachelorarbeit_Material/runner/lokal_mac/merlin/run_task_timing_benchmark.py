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

PIPELINE_ROOT = HOME / "merlin_pipeline_benchmark"
SCATTER_ROOT = HOME / "merlin_scatter_gather_benchmark"

RUN_DATA_ROOT = (
    HOME
    / "wms_benchmark_reference"
    / "run_data"
    / "merlin"
    / "task_timing_benchmark"
)

RESULTS_ROOT = (
    HOME
    / "wms_benchmark_reference"
    / "results"
    / "merlin_task_timing"
)

RAW_FILE = RESULTS_ROOT / "merlin_raw_results.csv"
CENTRAL_FILE = RESULTS_ROOT / "merlin_central_results.csv"
COORDINATION_FILE = RESULTS_ROOT / "merlin_coordination_results.csv"

SYSTEM = "Merlin"

WORKLOAD_REPETITIONS = {
    "short": 5,
    "medium": 5,
    "long": 3,
}

COMPUTE_SCRIPTS = {
    "short": "compute.py",
    "medium": "compute_medium.py",
    "long": "compute_long.py",
}

RAW_COLUMNS = [
    "system",
    "pattern",
    "workload",
    "chunks",
    "repetition",
    "ref_makespan_s",
    "wms_makespan_s",
    "overhead_s",
    "overhead_pct",
    "ratio",
    "ref_compute_phase_s",
    "wms_compute_phase_s",
    "wms_task_span_s",
    "wms_gen_to_pre_s",
    "wms_pre_to_comp_s",
    "wms_comp_to_post_s",
    "wms_pre_to_split_s",
    "wms_split_to_comp_s",
    "wms_comp_to_agg_s",
    "wms_agg_to_post_s",
    "wms_start_spread_s",
]

CENTRAL_COLUMNS = [
    "system",
    "pattern",
    "workload",
    "chunks",
    "ref_makespan_s",
    "wms_makespan_s",
    "overhead_s",
    "overhead_pct",
    "ratio",
    "ref_compute_phase_s",
    "wms_compute_phase_s",
]

COORDINATION_COLUMNS = [
    "system",
    "pattern",
    "workload",
    "chunks",
    "wms_task_span_s",
    "wms_gen_to_pre_s",
    "wms_pre_to_comp_s",
    "wms_comp_to_post_s",
    "wms_pre_to_split_s",
    "wms_split_to_comp_s",
    "wms_comp_to_agg_s",
    "wms_agg_to_post_s",
    "wms_start_spread_s",
    "wms_compute_phase_s",
]


def benchmark_environment() -> dict[str, str]:
    environment = dict(**__import__("os").environ)

    environment.update(
        {
            "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "VECLIB_MAXIMUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        }
    )

    return environment


def as_float(value: str | float | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def seconds(start_ns: int, end_ns: int) -> float:
    return (end_ns - start_ns) / 1_000_000_000


def write_csv(path: Path, columns: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def read_raw_rows() -> list[dict[str, str]]:
    if not RAW_FILE.exists():
        return []

    with RAW_FILE.open(newline="") as handle:
        return list(csv.DictReader(handle))


def run_command(command: list[str], cwd: Path) -> None:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=benchmark_environment(),
        text=True,
        capture_output=True,
    )

    if completed.returncode != 0:
        print(completed.stdout)
        print(completed.stderr, file=sys.stderr)
        raise RuntimeError(f"Fehler bei: {' '.join(command)}")


def read_timing_file(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}

    for line in path.read_text().splitlines():
        key, value = line.split("=", 1)
        values[key] = int(value) if key != "task_name" else value

    return values


def read_task_timings(directory: Path, pattern: str, chunks: int) -> dict[str, dict[str, int]]:
    task_paths: dict[str, Path] = {}

    if pattern == "Pipeline":
        task_paths = {
            "generate_input": directory / "generate_input" / "timing_generate_input.txt",
            "preprocess": directory / "preprocess" / "timing_preprocess.txt",
            "compute_1": directory / "compute_1" / "timing_compute_1.txt",
            "postprocess": directory / "postprocess" / "timing_postprocess.txt",
        }
    else:
        task_paths = {
            "generate_input": directory / "generate_input" / "timing_generate_input.txt",
            "preprocess": directory / "preprocess" / "timing_preprocess.txt",
            "split": directory / "split" / "timing_split.txt",
            "aggregate": directory / "aggregate" / "timing_aggregate.txt",
            "postprocess": directory / "postprocess" / "timing_postprocess.txt",
        }

        for index in range(1, chunks + 1):
            chunk_dir = directory / "compute" / f"CHUNK.{index}"
            treffer = sorted(chunk_dir.glob("timing_compute*.txt"))

            if not treffer:
                raise RuntimeError(
                    f"Keine Timing-Datei in {chunk_dir}"
                )

            task_paths[f"compute_{index}"] = treffer[0]

    missing = [str(path) for path in task_paths.values() if not path.exists()]

    if missing:
        raise RuntimeError(
            "Fehlende Timing-Dateien:\n" + "\n".join(missing)
        )

    return {
        task_name: read_timing_file(path)
        for task_name, path in task_paths.items()
    }


def read_reference_timings(directory: Path, pattern: str, chunks: int) -> dict[str, dict[str, int]]:
    task_names = ["generate_input", "preprocess", "compute_1", "postprocess"]

    if pattern == "Scatter-Gather":
        task_names = [
            "generate_input",
            "preprocess",
            "split",
            *[f"compute_{index}" for index in range(1, chunks + 1)],
            "aggregate",
            "postprocess",
        ]

    timings = {}

    for task_name in task_names:
        path = directory / f"timing_{task_name}.txt"

        if not path.exists():
            raise RuntimeError(f"Fehlende Referenz-Timing-Datei: {path}")

        timings[task_name] = read_timing_file(path)

    return timings


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


def reference_compute_phase(
    pattern: str,
    chunks: int,
    timings: dict[str, dict[str, int]],
) -> float | str:
    if pattern == "Pipeline":
        return ""

    compute_names = [f"compute_{index}" for index in range(1, chunks + 1)]

    earliest_start = min(
        timings[name]["task_start_ns"]
        for name in compute_names
    )

    latest_end = max(
        timings[name]["task_end_ns"]
        for name in compute_names
    )

    return seconds(earliest_start, latest_end)


def format_number(value: float | str | None) -> str:
    if value == "" or value is None:
        return ""

    return f"{float(value):.9f}"


def rebuild_summary_files(raw_rows: list[dict[str, str]]) -> None:
    groups: dict[tuple[str, str, str, str], list[dict[str, str]]] = defaultdict(list)

    for row in raw_rows:
        key = (
            row["system"],
            row["pattern"],
            row["workload"],
            row["chunks"],
        )
        groups[key].append(row)

    central_rows: list[dict[str, str]] = []
    coordination_rows: list[dict[str, str]] = []

    for key in sorted(groups):
        system, pattern, workload, chunks = key
        rows = groups[key]

        def median_column(column: str) -> float | str:
            values = [
                as_float(row[column])
                for row in rows
                if row.get(column, "") != ""
            ]

            return median(values) if values else ""

        ref_makespan = median_column("ref_makespan_s")
        wms_makespan = median_column("wms_makespan_s")

        if ref_makespan == "" or wms_makespan == "":
            raise RuntimeError("Unvollständige Makespan-Daten.")

        overhead = wms_makespan - ref_makespan
        overhead_pct = overhead / ref_makespan * 100
        ratio = wms_makespan / ref_makespan

        central_rows.append(
            {
                "system": system,
                "pattern": pattern,
                "workload": workload,
                "chunks": chunks,
                "ref_makespan_s": format_number(ref_makespan),
                "wms_makespan_s": format_number(wms_makespan),
                "overhead_s": format_number(overhead),
                "overhead_pct": format_number(overhead_pct),
                "ratio": format_number(ratio),
                "ref_compute_phase_s": format_number(
                    median_column("ref_compute_phase_s")
                ),
                "wms_compute_phase_s": format_number(
                    median_column("wms_compute_phase_s")
                ),
            }
        )

        coordination_rows.append(
            {
                "system": system,
                "pattern": pattern,
                "workload": workload,
                "chunks": chunks,
                "wms_task_span_s": format_number(median_column("wms_task_span_s")),
                "wms_gen_to_pre_s": format_number(median_column("wms_gen_to_pre_s")),
                "wms_pre_to_comp_s": format_number(median_column("wms_pre_to_comp_s")),
                "wms_comp_to_post_s": format_number(
                    median_column("wms_comp_to_post_s")
                ),
                "wms_pre_to_split_s": format_number(
                    median_column("wms_pre_to_split_s")
                ),
                "wms_split_to_comp_s": format_number(
                    median_column("wms_split_to_comp_s")
                ),
                "wms_comp_to_agg_s": format_number(
                    median_column("wms_comp_to_agg_s")
                ),
                "wms_agg_to_post_s": format_number(
                    median_column("wms_agg_to_post_s")
                ),
                "wms_start_spread_s": format_number(
                    median_column("wms_start_spread_s")
                ),
                "wms_compute_phase_s": format_number(
                    median_column("wms_compute_phase_s")
                ),
            }
        )

    write_csv(CENTRAL_FILE, CENTRAL_COLUMNS, central_rows)
    write_csv(COORDINATION_FILE, COORDINATION_COLUMNS, coordination_rows)


def selected_configurations() -> list[tuple[str, str, int]]:
    configurations = []

    for workload in ("short", "medium", "long"):
        configurations.append(("Pipeline", workload, 1))

    for workload in ("short", "medium", "long"):
        for chunks in (1, 2, 4):
            configurations.append(("Scatter-Gather", workload, chunks))

    return configurations


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Finaler Merlin Task-Timing-Benchmark."
    )

    parser.add_argument(
        "--only",
        choices=["all", "pipeline", "scatter"],
        default="all",
        help="Beschränkt den Lauf auf einen Workflow-Typ.",
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=None,
        help="Optional: überschreibt die Wiederholungszahl für einen Testlauf.",
    )

    parser.add_argument(
        "--workload",
        choices=["short", "medium", "long"],
        default=None,
        help="Beschränkt den Lauf auf einen Workload.",
    )

    parser.add_argument(
        "--chunks",
        type=int,
        choices=[1, 2, 4],
        default=None,
        help="Beschränkt Scatter-Gather auf eine Chunk-Anzahl.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Führt vorhandene Wiederholungen erneut aus.",
    )

    args = parser.parse_args()

    if shutil.which("merlin") is None:
        raise RuntimeError(
            "Merlin wurde nicht gefunden. Aktiviere zuerst ~/merlin_venv."
        )

    existing_rows = read_raw_rows()

    existing_keys = {
        (
            row["pattern"],
            row["workload"],
            int(row["chunks"]),
            int(row["repetition"]),
        )
        for row in existing_rows
    }

    if args.force:
        existing_rows = []

    configurations = selected_configurations()

    for pattern, workload, chunks in configurations:
        if args.only == "pipeline" and pattern != "Pipeline":
            continue

        if args.only == "scatter" and pattern != "Scatter-Gather":
            continue

        if args.workload is not None and workload != args.workload:
            continue

        if args.chunks is not None and chunks != args.chunks:
            continue

        repetitions = args.repetitions or WORKLOAD_REPETITIONS[workload]

        for repetition in range(1, repetitions + 1):
            key = (pattern, workload, chunks, repetition)

            if key in existing_keys and not args.force:
                print(
                    f"[SKIP] {pattern} | {workload} | chunks={chunks} | rep={repetition}"
                )
                continue

            print(
                f"\n[START] {pattern} | {workload} | chunks={chunks} | rep={repetition}"
            )

            repetition_root = (
                RUN_DATA_ROOT
                / pattern.lower().replace("-", "_").replace(" ", "_")
                / workload
                / f"chunks_{chunks}"
                / f"rep_{repetition}"
            )

            reference_workspace = repetition_root / "reference"
            merlin_workspace_root = repetition_root / "merlin"

            if reference_workspace.exists():
                shutil.rmtree(reference_workspace)

            reference_workspace.mkdir(parents=True, exist_ok=True)

            ref_start_ns = time.time_ns()

            if pattern == "Pipeline":
                ref_timings = direct_reference_pipeline(
                    reference_workspace,
                    workload,
                )
            else:
                ref_timings = direct_reference_scatter(
                    reference_workspace,
                    workload,
                    chunks,
                )

            ref_end_ns = time.time_ns()
            ref_makespan = seconds(ref_start_ns, ref_end_ns)

            if pattern == "Pipeline":
                yaml_name = f"pipeline_{workload}.yaml"
                project_root = PIPELINE_ROOT
            else:
                yaml_name = f"scatter_gather_{chunks}_{workload}.yaml"
                project_root = SCATTER_ROOT

            wms_makespan, wms_timings = run_merlin_workflow(
                project_root=project_root,
                yaml_name=yaml_name,
                destination=merlin_workspace_root,
                pattern=pattern,
                chunks=chunks,
            )

            wms_metrics = metric_values(pattern, chunks, wms_timings)

            ref_phase = reference_compute_phase(
                pattern,
                chunks,
                ref_timings,
            )

            overhead = wms_makespan - ref_makespan
            overhead_pct = overhead / ref_makespan * 100
            ratio = wms_makespan / ref_makespan

            row = {
                "system": SYSTEM,
                "pattern": pattern,
                "workload": workload,
                "chunks": str(chunks),
                "repetition": str(repetition),
                "ref_makespan_s": format_number(ref_makespan),
                "wms_makespan_s": format_number(wms_makespan),
                "overhead_s": format_number(overhead),
                "overhead_pct": format_number(overhead_pct),
                "ratio": format_number(ratio),
                "ref_compute_phase_s": format_number(ref_phase),
                "wms_compute_phase_s": format_number(
                    wms_metrics["wms_compute_phase_s"]
                ),
                "wms_task_span_s": format_number(wms_metrics["wms_task_span_s"]),
                "wms_gen_to_pre_s": format_number(wms_metrics["wms_gen_to_pre_s"]),
                "wms_pre_to_comp_s": format_number(
                    wms_metrics["wms_pre_to_comp_s"]
                ),
                "wms_comp_to_post_s": format_number(
                    wms_metrics["wms_comp_to_post_s"]
                ),
                "wms_pre_to_split_s": format_number(
                    wms_metrics["wms_pre_to_split_s"]
                ),
                "wms_split_to_comp_s": format_number(
                    wms_metrics["wms_split_to_comp_s"]
                ),
                "wms_comp_to_agg_s": format_number(
                    wms_metrics["wms_comp_to_agg_s"]
                ),
                "wms_agg_to_post_s": format_number(
                    wms_metrics["wms_agg_to_post_s"]
                ),
                "wms_start_spread_s": format_number(
                    wms_metrics["wms_start_spread_s"]
                ),
            }

            existing_rows.append(row)
            write_csv(RAW_FILE, RAW_COLUMNS, existing_rows)
            rebuild_summary_files(existing_rows)

            print(
                f"[FERTIG] Referenz={ref_makespan:.3f}s | "
                f"Merlin={wms_makespan:.3f}s | "
                f"Overhead={overhead:.3f}s"
            )

    rebuild_summary_files(existing_rows)

    print("\nBenchmark abgeschlossen.")
    print(f"Rohdaten:        {RAW_FILE}")
    print(f"Zentrale Werte:  {CENTRAL_FILE}")
    print(f"Koordination:    {COORDINATION_FILE}")


if __name__ == "__main__":
    main()
