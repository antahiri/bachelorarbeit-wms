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
            "--cleanup", "none",
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
    scratch_run = root / "scratch" / run_dir.relative_to(root / "submit")
    timings = read_timing_files(scratch_run)

    return makespan, summary, run_dir, timings


RAW_FIELDS = [
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

CENTRAL_FIELDS = [
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


def raw_row(pattern, workload, chunks, repetition, ref_makespan, wms_makespan,
            ref_compute_phase, wms_compute_phase, coordination):
    overhead = wms_makespan - ref_makespan

    row = {
        "system": "pegasus",
        "pattern": pattern,
        "workload": workload,
        "chunks": chunks,
        "repetition": repetition,
        "ref_makespan_s": f"{ref_makespan:.9f}",
        "wms_makespan_s": f"{wms_makespan:.9f}",
        "overhead_s": f"{overhead:.9f}",
        "overhead_pct": f"{(overhead / ref_makespan) * 100:.6f}",
        "ratio": f"{wms_makespan / ref_makespan:.9f}",
        "ref_compute_phase_s": (
            f"{ref_compute_phase:.9f}" if ref_compute_phase is not None else ""
        ),
        "wms_compute_phase_s": (
            f"{wms_compute_phase:.9f}" if wms_compute_phase is not None else ""
        ),
    }

    for key, value in coordination.items():
        if key != "wms_compute_phase_s":
            row[key] = f"{value:.9f}"

    return row


def create_central_rows(raw_rows):
    grouped = {}

    for row in raw_rows:
        key = (row["pattern"], row["workload"], int(row["chunks"]))
        grouped.setdefault(key, []).append(row)

    ordered = sorted(
        grouped,
        key=lambda item: (
            0 if item[0] == "pipeline" else 1,
            ["short", "medium", "long"].index(item[1]),
            item[2],
        ),
    )

    central_rows = []

    for pattern, workload, chunks in ordered:
        rows = grouped[(pattern, workload, chunks)]

        ref_median = statistics.median(float(row["ref_makespan_s"]) for row in rows)
        wms_median = statistics.median(float(row["wms_makespan_s"]) for row in rows)
        overhead = wms_median - ref_median

        central = {
            "system": "pegasus",
            "pattern": pattern,
            "workload": workload,
            "chunks": chunks,
            "ref_makespan_s": f"{ref_median:.9f}",
            "wms_makespan_s": f"{wms_median:.9f}",
            "overhead_s": f"{overhead:.9f}",
            "overhead_pct": f"{(overhead / ref_median) * 100:.6f}",
            "ratio": f"{wms_median / ref_median:.9f}",
            "ref_compute_phase_s": "",
            "wms_compute_phase_s": "",
        }

        ref_phases = [
            float(row["ref_compute_phase_s"])
            for row in rows
            if row["ref_compute_phase_s"]
        ]
        wms_phases = [
            float(row["wms_compute_phase_s"])
            for row in rows
            if row["wms_compute_phase_s"]
        ]

        if ref_phases:
            central["ref_compute_phase_s"] = f"{statistics.median(ref_phases):.9f}"
        if wms_phases:
            central["wms_compute_phase_s"] = f"{statistics.median(wms_phases):.9f}"

        central_rows.append(central)

    return central_rows


def reference_compute_phase(compute_dirs):
    starts = []
    ends = []

    for index, compute_dir in enumerate(compute_dirs, start=1):
        values = {}

        for line in (compute_dir / f"timing_compute_{index}.txt").read_text().splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()

        starts.append(int(values["task_start_ns"]))
        ends.append(int(values["task_end_ns"]))

    return (max(ends) - min(starts)) / 1_000_000_000


def read_timing_files(scratch_dir: Path):
    timings = {}

    for path in scratch_dir.rglob("timing_*.txt"):
        values = {}

        for line in path.read_text().splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()

        name = values["task_name"]

        if name in timings:
            raise RuntimeError(f"Doppelte Timing-Daten fuer {name} in {scratch_dir}")

        timings[name] = {
            "start": int(values["task_start_ns"]),
            "end": int(values["task_end_ns"]),
        }

    if not timings:
        raise RuntimeError(f"Keine Timing-Dateien in {scratch_dir}")

    return timings


def pipeline_coordination(timings):
    expected = ["generate_input", "preprocess", "compute_1", "postprocess"]

    for name in expected:
        if name not in timings:
            raise RuntimeError(f"Timing fehlt: {name}")

    def gap(first, second):
        return (timings[second]["start"] - timings[first]["end"]) / 1_000_000_000

    return {
        "wms_task_span_s": (
            timings["postprocess"]["end"] - timings["generate_input"]["start"]
        ) / 1_000_000_000,
        "wms_gen_to_pre_s": gap("generate_input", "preprocess"),
        "wms_pre_to_comp_s": gap("preprocess", "compute_1"),
        "wms_comp_to_post_s": gap("compute_1", "postprocess"),
    }


def scatter_coordination(timings, chunks: int):
    expected = ["generate_input", "preprocess", "split", "aggregate", "postprocess"]
    expected += [f"compute_{index}" for index in range(1, chunks + 1)]

    for name in expected:
        if name not in timings:
            raise RuntimeError(f"Timing fehlt: {name}")

    computes = [timings[f"compute_{index}"] for index in range(1, chunks + 1)]

    first_compute_start = min(item["start"] for item in computes)
    last_compute_start = max(item["start"] for item in computes)
    last_compute_end = max(item["end"] for item in computes)

    def gap(first, second):
        return (timings[second]["start"] - timings[first]["end"]) / 1_000_000_000

    return {
        "wms_task_span_s": (
            timings["postprocess"]["end"] - timings["generate_input"]["start"]
        ) / 1_000_000_000,
        "wms_gen_to_pre_s": gap("generate_input", "preprocess"),
        "wms_pre_to_split_s": gap("preprocess", "split"),
        "wms_split_to_comp_s": (
            first_compute_start - timings["split"]["end"]
        ) / 1_000_000_000,
        "wms_comp_to_agg_s": (
            timings["aggregate"]["start"] - last_compute_end
        ) / 1_000_000_000,
        "wms_agg_to_post_s": gap("aggregate", "postprocess"),
        "wms_start_spread_s": (
            last_compute_start - first_compute_start
        ) / 1_000_000_000,
        "wms_compute_phase_s": (
            last_compute_end - first_compute_start
        ) / 1_000_000_000,
    }


COORDINATION_FIELDS = [
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


def write_coordination_file(rows, path: Path):
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=COORDINATION_FIELDS)
        writer.writeheader()

        for row in rows:
            writer.writerow({field: row.get(field, "") for field in COORDINATION_FIELDS})


def coordination_medians(system, pattern, workload, chunks, samples):
    row = {
        "system": system,
        "pattern": pattern,
        "workload": workload,
        "chunks": chunks,
    }

    keys = set()
    for sample in samples:
        keys.update(sample.keys())

    for key in keys:
        values = [sample[key] for sample in samples if key in sample]
        row[key] = f"{statistics.median(values):.9f}"

    return row


def paired_pipeline_benchmark(root: Path, workload: str, repetitions: int,
                              checksum: str, raw_writer, raw_file, archive_root: Path):
    scripts = root / "benchmark_scripts"
    coordination_samples = []

    for repetition in range(1, repetitions + 1):
        with tempfile.TemporaryDirectory(prefix="pegasus_pipeline_reference_") as temp:
            workdir = Path(temp)

            start = time.perf_counter()

            run([scripts / "generate_input.py", "raw_input.txt"], workdir)
            run([scripts / "preprocess.py", "raw_input.txt", "prepared_input.txt"], workdir)
            run([scripts / "compute.py", "prepared_input.txt", "result.txt"], workdir)
            run([scripts / "postprocess.py", "result.txt", "summary.txt"], workdir)

            ref_makespan = time.perf_counter() - start
            summary = (workdir / "summary.txt").read_text()

            validate_pipeline(summary, checksum)

        print(f"[Referenz Pipeline] Wiederholung {repetition}/{repetitions}: {ref_makespan:.3f} s")

        wms_makespan, summary, run_dir, timings = run_pegasus_workflow(root)

        validate_pipeline(summary, checksum)
        coordination = pipeline_coordination(timings)
        coordination_samples.append(coordination)

        archive = archive_root / f"pipeline_rep_{repetition}"
        if archive.exists():
            shutil.rmtree(archive)
        shutil.copytree(run_dir, archive)

        raw_writer.writerow(
            raw_row(
                "pipeline", workload, 1, repetition,
                ref_makespan, wms_makespan, None, None, coordination,
            )
        )
        raw_file.flush()

        print(f"[Pegasus Pipeline] Wiederholung {repetition}/{repetitions}: {wms_makespan:.3f} s")

    return coordination_samples


def paired_scatter_benchmark(root: Path, workload: str, repetitions: int,
                             checksum: str, raw_writer, raw_file, archive_root: Path):
    scripts = root / "benchmark_scripts"
    coordination_samples = {}

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

                ref_makespan = time.perf_counter() - start
                summary = (workdir / "summary.txt").read_text()

                validate_scatter(summary, chunks, checksum)

                ref_compute_phase = reference_compute_phase(compute_dirs)

            print(
                f"[Referenz Scatter-Gather] chunks={chunks}, "
                f"Wiederholung {repetition}/{repetitions}: {ref_makespan:.3f} s"
            )

            wms_makespan, summary, run_dir, timings = run_pegasus_workflow(root, chunks)

            validate_scatter(summary, chunks, checksum)
            coordination = scatter_coordination(timings, chunks)
            coordination_samples.setdefault(chunks, []).append(coordination)

            wms_compute_phase = coordination["wms_compute_phase_s"]

            archive = archive_root / f"scatter_chunks_{chunks}_rep_{repetition}"
            if archive.exists():
                shutil.rmtree(archive)
            shutil.copytree(run_dir, archive)

            raw_writer.writerow(
                raw_row(
                    "scatter_gather", workload, chunks, repetition,
                    ref_makespan, wms_makespan,
                    ref_compute_phase, wms_compute_phase, coordination,
                )
            )
            raw_file.flush()

            print(
                f"[Pegasus Scatter-Gather] chunks={chunks}, "
                f"Wiederholung {repetition}/{repetitions}: "
                f"{wms_makespan:.3f} s, Compute-Phase={wms_compute_phase:.3f} s"
            )

    return coordination_samples


def write_rows(path: Path, fieldnames, rows):
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


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

    RESULT_ROOT.mkdir(parents=True, exist_ok=True)

    raw_path = RESULT_ROOT / "pegasus_raw_results.csv"
    coordination_rows = []
    raw_rows_all = []

    with raw_path.open("w", newline="") as raw_file:
        raw_writer = csv.DictWriter(raw_file, fieldnames=RAW_FIELDS)
        raw_writer.writeheader()

        class CollectingWriter:
            def writerow(self, row):
                raw_rows_all.append(row)
                raw_writer.writerow({field: row.get(field, "") for field in RAW_FIELDS})

        collecting_writer = CollectingWriter()

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

            archive_root = WORK_ROOT / workload
            archive_root.mkdir(parents=True, exist_ok=True)

            print("\n--- Pipeline: Referenz und Pegasus je Wiederholung ---")
            pipeline_samples = paired_pipeline_benchmark(
                pipeline_root, workload, repetitions, checksum,
                collecting_writer, raw_file, archive_root,
            )

            print("\n--- Scatter-Gather: Referenz und Pegasus je Wiederholung ---")
            scatter_samples = paired_scatter_benchmark(
                scatter_root, workload, repetitions, checksum,
                collecting_writer, raw_file, archive_root,
            )

            coordination_rows.append(
                coordination_medians("pegasus", "pipeline", workload, 1, pipeline_samples)
            )

            for chunks in [1, 2, 4]:
                coordination_rows.append(
                    coordination_medians(
                        "pegasus", "scatter_gather", workload, chunks,
                        scatter_samples[chunks],
                    )
                )

            print(f"\n{workload.upper()} abgeschlossen.")

    central_path = RESULT_ROOT / "pegasus_central_results.csv"
    write_rows(central_path, CENTRAL_FIELDS, create_central_rows(raw_rows_all))

    coordination_path = RESULT_ROOT / "pegasus_coordination_results.csv"
    write_rows(coordination_path, COORDINATION_FIELDS, coordination_rows)

    print("\n" + "=" * 76)
    print("ALLE PEGASUS-MESSUNGEN ABGESCHLOSSEN")
    print("=" * 76)
    print(raw_path)
    print(central_path)
    print(coordination_path)
    print()
    print(central_path.read_text())


if __name__ == "__main__":
    main()
