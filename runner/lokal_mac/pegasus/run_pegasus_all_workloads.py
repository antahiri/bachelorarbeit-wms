#!/usr/bin/env python3

import csv
import re
import shutil
import statistics
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


HOME = Path.home()
PYTHON = Path("/usr/bin/python3")

PIPELINE_SOURCE = HOME / "pegasus_pipeline_benchmark"
SCATTER_SOURCE = HOME / "pegasus_scatter_gather_benchmark"

RESULT_ROOT = HOME / "wms_benchmark_reference" / "results" / "pegasus"
WORK_ROOT = HOME / "wms_benchmark_reference" / "run_data" / "pegasus"

WORKLOADS = {
    "short": {
        "repeat_factor": 1,
        "repetitions": 5,
        "checksum": "4522800285",
    },
    "medium": {
        "repeat_factor": 13,
        "repetitions": 5,
        "checksum": "58796403705",
    },
    "long": {
        "repeat_factor": 60,
        "repetitions": 3,
        "checksum": "271368017100",
    },
}

PIPELINE_EXPECTED_BASE = {
    "count": "71",
    "sum": "1353",
    "mean": "19.06",
}

SCATTER_EXPECTED_BASE = {
    "total_count": "71",
    "total_sum": "1353",
    "global_mean": "19.06",
}


def run(command, cwd, capture=True):
    return subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        text=True,
        capture_output=capture,
        check=True,
    )


def prepare_copy(source: Path, target: Path, repeat_factor: int):
    if target.exists():
        shutil.rmtree(target)

    shutil.copytree(source, target)

    for unwanted in [
        "submit",
        "scratch",
        "output",
        "archived_benchmark_runs",
        "submit_timing_chunks_2_rep_3_20260630_040256",
    ]:
        path = target / unwanted
        if path.exists():
            shutil.rmtree(path)

    old_root = str(source)
    new_root = str(target)

    for path in [target / "sites.yml"]:
        text = path.read_text()
        path.write_text(text.replace(old_root, new_root))

    compute_path = target / "benchmark_scripts" / "compute.py"
    text = compute_path.read_text()

    old = """for value in values:
    matrix_result += matrix_checksum(value)
"""

    new = f"""for _ in range({repeat_factor}):
    for value in values:
        matrix_result += matrix_checksum(value)
"""

    if old not in text:
        raise RuntimeError(f"Compute-Schleife nicht gefunden: {compute_path}")

    compute_path.write_text(text.replace(old, new))


def remove_runtime_directories(root: Path):
    for name in ["submit", "scratch", "output"]:
        path = root / name
        if path.exists():
            shutil.rmtree(path)

    (root / "scratch").mkdir(parents=True, exist_ok=True)
    (root / "output").mkdir(parents=True, exist_ok=True)


def get_run_dir(submit_dir: Path):
    candidates = [
        path
        for path in submit_dir.rglob("run*")
        if path.is_dir() and re.fullmatch(r"run\d+", path.name)
    ]

    if not candidates:
        raise RuntimeError(f"Kein Pegasus-Run-Verzeichnis gefunden: {submit_dir}")

    return max(candidates, key=lambda path: path.stat().st_mtime)


def wait_for_success(run_dir: Path, timeout_seconds=7200):
    deadline = time.monotonic() + timeout_seconds

    while True:
        result = subprocess.run(
            ["pegasus-status", "-l", str(run_dir)],
            text=True,
            capture_output=True,
        )

        output = result.stdout + result.stderr

        if "Failure" in output or "Failed" in output:
            raise RuntimeError(f"Pegasus-Workflow fehlgeschlagen:\n{output}")

        if "Success" in output:
            return

        if time.monotonic() > deadline:
            raise TimeoutError(f"Pegasus-Timeout:\n{output}")

        time.sleep(0.5)


def get_summary_file(output_dir: Path):
    summaries = list(output_dir.rglob("summary.txt"))

    if not summaries:
        raise RuntimeError(f"summary.txt fehlt in {output_dir}")

    return max(summaries, key=lambda path: path.stat().st_mtime)


def validate_pipeline(summary: str, checksum: str):
    expected = {
        **PIPELINE_EXPECTED_BASE,
        "matrix_checksum": checksum,
    }

    for key, value in expected.items():
        if f"{key}={value}" not in summary:
            raise RuntimeError(f"Pipeline-Validierung fehlgeschlagen: {key}={value}\n\n{summary}")


def validate_scatter(summary: str, chunks: int, checksum: str):
    expected = {
        **SCATTER_EXPECTED_BASE,
        "chunks": str(chunks),
        "total_matrix_checksum": checksum,
    }

    for key, value in expected.items():
        if f"{key}={value}" not in summary:
            raise RuntimeError(f"Scatter-Gather-Validierung fehlgeschlagen: {key}={value}\n\n{summary}")


def run_pegasus_workflow(root: Path, chunks=None):
    remove_runtime_directories(root)

    build_command = ["/usr/bin/env", "python3", "build_workflow.py"]
    if chunks is not None:
        build_command.append(str(chunks))

    run(build_command, root)

    start = time.perf_counter()

    run(
        [
            "pegasus-plan",
            "--conf", "pegasus.properties",
            "--dir", "submit",
            "--sites", "condorpool",
            "--output-sites", "local",
            "--cleanup", "leaf",
            "--force",
            "workflow.yml",
        ],
        root,
    )

    run_dir = get_run_dir(root / "submit")

    run(["pegasus-run", str(run_dir)], root)

    wait_for_success(run_dir)

    makespan = time.perf_counter() - start
    summary = get_summary_file(root / "output").read_text()

    return makespan, summary, run_dir


def direct_pipeline_reference(root: Path, repetitions: int, checksum: str, output: Path):
    scripts = root / "benchmark_scripts"

    with output.open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "system",
                "workflow_pattern",
                "repetition",
                "makespan_seconds",
            ],
        )
        writer.writeheader()

        for repetition in range(1, repetitions + 1):
            with tempfile.TemporaryDirectory(prefix="pegasus_pipeline_reference_") as temp:
                workdir = Path(temp)

                start = time.perf_counter()

                run([scripts / "generate_input.py", "raw_input.txt"], workdir)
                run([scripts / "preprocess.py", "raw_input.txt", "prepared_input.txt"], workdir)
                run([scripts / "compute.py", "prepared_input.txt", "result.txt"], workdir)
                run([scripts / "postprocess.py", "result.txt", "summary.txt"], workdir)

                makespan = time.perf_counter() - start
                summary = (workdir / "summary.txt").read_text()

                validate_pipeline(summary, checksum)

                writer.writerow(
                    {
                        "system": "direct_reference",
                        "workflow_pattern": "pipeline",
                        "repetition": repetition,
                        "makespan_seconds": f"{makespan:.6f}",
                    }
                )
                file.flush()

                print(f"[Referenz Pipeline] Wiederholung {repetition}/{repetitions}: {makespan:.3f} s")


def direct_scatter_reference(root: Path, repetitions: int, checksum: str, output: Path):
    scripts = root / "benchmark_scripts"

    with output.open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "system",
                "workflow_pattern",
                "chunks",
                "repetition",
                "makespan_seconds",
            ],
        )
        writer.writeheader()

        for chunks in [1, 2, 4]:
            for repetition in range(1, repetitions + 1):
                with tempfile.TemporaryDirectory(prefix="pegasus_scatter_reference_") as temp:
                    workdir = Path(temp)

                    start = time.perf_counter()

                    run([scripts / "generate_input.py", "raw_input.txt"], workdir)
                    run([scripts / "preprocess.py", "raw_input.txt", "prepared_input.txt"], workdir)
                    run([scripts / "split.py", "prepared_input.txt", str(chunks)], workdir)

                    compute_dirs = []

                    for index in range(1, chunks + 1):
                        compute_dir = workdir / f"compute_{index}"
                        compute_dir.mkdir()

                        shutil.copy2(
                            workdir / f"chunk_{index}.txt",
                            compute_dir / f"chunk_{index}.txt",
                        )

                        compute_dirs.append(compute_dir)

                    def compute_one(index_and_dir):
                        index, compute_dir = index_and_dir

                        run(
                            [
                                scripts / "compute.py",
                                f"chunk_{index}.txt",
                                f"result_{index}.txt",
                            ],
                            compute_dir,
                        )

                    with ThreadPoolExecutor(max_workers=chunks) as executor:
                        list(executor.map(compute_one, enumerate(compute_dirs, start=1)))

                    aggregate_command = [
                        scripts / "aggregate.py",
                        *[
                            compute_dir / f"result_{index}.txt"
                            for index, compute_dir in enumerate(compute_dirs, start=1)
                        ],
                        "aggregated_result.txt",
                    ]

                    run(aggregate_command, workdir)
                    run(
                        [
                            scripts / "postprocess.py",
                            "aggregated_result.txt",
                            "summary.txt",
                        ],
                        workdir,
                    )

                    makespan = time.perf_counter() - start
                    summary = (workdir / "summary.txt").read_text()

                    validate_scatter(summary, chunks, checksum)

                    writer.writerow(
                        {
                            "system": "direct_reference",
                            "workflow_pattern": "scatter_gather",
                            "chunks": chunks,
                            "repetition": repetition,
                            "makespan_seconds": f"{makespan:.6f}",
                        }
                    )
                    file.flush()

                    print(
                        f"[Referenz Scatter-Gather] chunks={chunks}, "
                        f"Wiederholung {repetition}/{repetitions}: {makespan:.3f} s"
                    )


def run_pipeline_benchmark(root: Path, repetitions: int, checksum: str, output: Path, archive_root: Path):
    with output.open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "system",
                "workflow_pattern",
                "chunks",
                "repetition",
                "makespan_seconds",
            ],
        )
        writer.writeheader()

        for repetition in range(1, repetitions + 1):
            makespan, summary, run_dir = run_pegasus_workflow(root)

            validate_pipeline(summary, checksum)

            archive = archive_root / f"pipeline_rep_{repetition}"
            if archive.exists():
                shutil.rmtree(archive)
            shutil.copytree(run_dir, archive)

            writer.writerow(
                {
                    "system": "pegasus",
                    "workflow_pattern": "pipeline",
                    "chunks": 1,
                    "repetition": repetition,
                    "makespan_seconds": f"{makespan:.6f}",
                }
            )
            file.flush()

            print(f"[Pegasus Pipeline] Wiederholung {repetition}/{repetitions}: {makespan:.3f} s")


def run_scatter_benchmark(root: Path, repetitions: int, checksum: str, output: Path, archive_root: Path):
    with output.open("w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "system",
                "workflow_pattern",
                "chunks",
                "repetition",
                "makespan_seconds",
                "compute_phase_seconds",
                "compute_task_throughput_tasks_per_second",
            ],
        )
        writer.writeheader()

        for chunks in [1, 2, 4]:
            for repetition in range(1, repetitions + 1):
                makespan, summary, run_dir = run_pegasus_workflow(root, chunks)

                validate_scatter(summary, chunks, checksum)

                start_match = re.search(r"compute_start_ns=(\d+)", summary)
                end_match = re.search(r"compute_end_ns=(\d+)", summary)

                if start_match is None or end_match is None:
                    raise RuntimeError(f"Compute-Zeitstempel fehlen:\n{summary}")

                compute_phase = (
                    int(end_match.group(1)) - int(start_match.group(1))
                ) / 1_000_000_000

                if compute_phase <= 0:
                    raise RuntimeError(f"Ungültige Compute-Phase: {compute_phase}")

                throughput = chunks / compute_phase

                archive = archive_root / f"scatter_chunks_{chunks}_rep_{repetition}"
                if archive.exists():
                    shutil.rmtree(archive)
                shutil.copytree(run_dir, archive)

                writer.writerow(
                    {
                        "system": "pegasus",
                        "workflow_pattern": "scatter_gather",
                        "chunks": chunks,
                        "repetition": repetition,
                        "makespan_seconds": f"{makespan:.6f}",
                        "compute_phase_seconds": f"{compute_phase:.6f}",
                        "compute_task_throughput_tasks_per_second": f"{throughput:.6f}",
                    }
                )
                file.flush()

                print(
                    f"[Pegasus Scatter-Gather] chunks={chunks}, "
                    f"Wiederholung {repetition}/{repetitions}: "
                    f"{makespan:.3f} s, Compute-Phase={compute_phase:.3f} s"
                )


def create_pipeline_summary(workload: str, raw_path: Path, reference_path: Path, summary_path: Path):
    raw_rows = list(csv.DictReader(raw_path.open()))
    reference_rows = list(csv.DictReader(reference_path.open()))

    pegasus_median = statistics.median(float(row["makespan_seconds"]) for row in raw_rows)
    reference_median = statistics.median(float(row["makespan_seconds"]) for row in reference_rows)

    fields = central_fields()

    with summary_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerow(
            {
                "system": "pegasus",
                "workflow_pattern": "pipeline",
                "workload": workload,
                "chunks": 1,
                "reference_median_seconds": f"{reference_median:.6f}",
                "pegasus_median_seconds": f"{pegasus_median:.6f}",
                "workflow_execution_overhead_seconds": f"{pegasus_median - reference_median:.6f}",
                "wms_reference_ratio": f"{pegasus_median / reference_median:.6f}",
                "compute_phase_median_seconds": "",
                "speedup": "",
                "efficiency": "",
                "compute_task_throughput_tasks_per_second": "",
            }
        )


def create_scatter_summary(workload: str, raw_path: Path, reference_path: Path, summary_path: Path):
    raw_rows = list(csv.DictReader(raw_path.open()))
    reference_rows = list(csv.DictReader(reference_path.open()))

    raw_by_chunks = {}
    phase_by_chunks = {}
    reference_by_chunks = {}

    for row in raw_rows:
        chunks = int(row["chunks"])
        raw_by_chunks.setdefault(chunks, []).append(float(row["makespan_seconds"]))
        phase_by_chunks.setdefault(chunks, []).append(float(row["compute_phase_seconds"]))

    for row in reference_rows:
        chunks = int(row["chunks"])
        reference_by_chunks.setdefault(chunks, []).append(float(row["makespan_seconds"]))

    pegasus_medians = {
        chunks: statistics.median(values)
        for chunks, values in raw_by_chunks.items()
    }

    baseline = pegasus_medians[1]

    with summary_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=central_fields())
        writer.writeheader()

        for chunks in [1, 2, 4]:
            pegasus_median = pegasus_medians[chunks]
            reference_median = statistics.median(reference_by_chunks[chunks])
            compute_median = statistics.median(phase_by_chunks[chunks])

            speedup = baseline / pegasus_median
            efficiency = speedup / chunks
            throughput = chunks / compute_median

            writer.writerow(
                {
                    "system": "pegasus",
                    "workflow_pattern": "scatter_gather",
                    "workload": workload,
                    "chunks": chunks,
                    "reference_median_seconds": f"{reference_median:.6f}",
                    "pegasus_median_seconds": f"{pegasus_median:.6f}",
                    "workflow_execution_overhead_seconds": f"{pegasus_median - reference_median:.6f}",
                    "wms_reference_ratio": f"{pegasus_median / reference_median:.6f}",
                    "compute_phase_median_seconds": f"{compute_median:.6f}",
                    "speedup": f"{speedup:.6f}",
                    "efficiency": f"{efficiency:.6f}",
                    "compute_task_throughput_tasks_per_second": f"{throughput:.6f}",
                }
            )


def central_fields():
    return [
        "system",
        "workflow_pattern",
        "workload",
        "chunks",
        "reference_median_seconds",
        "pegasus_median_seconds",
        "workflow_execution_overhead_seconds",
        "wms_reference_ratio",
        "compute_phase_median_seconds",
        "speedup",
        "efficiency",
        "compute_task_throughput_tasks_per_second",
    ]


def create_central_file():
    rows = []

    for workload in ["short", "medium", "long"]:
        rows.extend(
            csv.DictReader(
                (RESULT_ROOT / "pipeline" / workload / "summary.csv").open()
            )
        )

    for workload in ["short", "medium", "long"]:
        rows.extend(
            csv.DictReader(
                (RESULT_ROOT / "scatter_gather" / workload / "summary.csv").open()
            )
        )

    central = RESULT_ROOT / "pegasus_central_results.csv"

    with central.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=central_fields())
        writer.writeheader()
        writer.writerows(rows)

    return central


def check_environment():
    result = subprocess.run(
        ["condor_status"],
        text=True,
        capture_output=True,
    )

    output = result.stdout + result.stderr

    if result.returncode != 0 or "slot" not in output.lower():
        raise RuntimeError(
            "HTCondor ist nicht verfügbar. Prüfe CONDOR_CONFIG und condor_master.\n\n"
            + output
        )


def main():
    check_environment()

    for workload, config in WORKLOADS.items():
        repetitions = config["repetitions"]
        repeat_factor = config["repeat_factor"]
        checksum = config["checksum"]

        print("\n" + "=" * 76)
        print(f"STARTE PEGASUS-WORKLOAD: {workload.upper()} | Faktor={repeat_factor}")
        print("=" * 76)

        pipeline_root = HOME / f"pegasus_pipeline_{workload}"
        scatter_root = HOME / f"pegasus_scatter_gather_{workload}"

        prepare_copy(PIPELINE_SOURCE, pipeline_root, repeat_factor)
        prepare_copy(SCATTER_SOURCE, scatter_root, repeat_factor)

        pipeline_result_dir = RESULT_ROOT / "pipeline" / workload
        scatter_result_dir = RESULT_ROOT / "scatter_gather" / workload

        pipeline_result_dir.mkdir(parents=True, exist_ok=True)
        scatter_result_dir.mkdir(parents=True, exist_ok=True)

        pipeline_reference = pipeline_result_dir / "reference_results.csv"
        pipeline_raw = pipeline_result_dir / "raw_results.csv"
        pipeline_summary = pipeline_result_dir / "summary.csv"

        scatter_reference = scatter_result_dir / "reference_results.csv"
        scatter_raw = scatter_result_dir / "raw_results.csv"
        scatter_summary = scatter_result_dir / "summary.csv"

        archive_root = WORK_ROOT / workload
        archive_root.mkdir(parents=True, exist_ok=True)

        print("\n--- Direkte Referenz: Pipeline ---")
        direct_pipeline_reference(
            pipeline_root,
            repetitions,
            checksum,
            pipeline_reference,
        )

        print("\n--- Direkte Referenz: Scatter-Gather ---")
        direct_scatter_reference(
            scatter_root,
            repetitions,
            checksum,
            scatter_reference,
        )

        print("\n--- Pegasus: Pipeline ---")
        run_pipeline_benchmark(
            pipeline_root,
            repetitions,
            checksum,
            pipeline_raw,
            archive_root,
        )

        print("\n--- Pegasus: Scatter-Gather ---")
        run_scatter_benchmark(
            scatter_root,
            repetitions,
            checksum,
            scatter_raw,
            archive_root,
        )

        create_pipeline_summary(
            workload,
            pipeline_raw,
            pipeline_reference,
            pipeline_summary,
        )

        create_scatter_summary(
            workload,
            scatter_raw,
            scatter_reference,
            scatter_summary,
        )

        print(f"\n{workload.upper()} abgeschlossen.")

    central = create_central_file()

    print("\n" + "=" * 76)
    print("ALLE PEGASUS-MESSUNGEN ABGESCHLOSSEN")
    print("=" * 76)
    print(central)
    print()
    print(central.read_text())


if __name__ == "__main__":
    main()
