#!/usr/bin/env python3

import argparse
import csv
import os
import shutil
import statistics
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from aiida import load_profile
from aiida.engine import submit
from aiida.orm import Int, SinglefileData, Str, load_code, load_node

from pipeline_calcjob_workchain import PipelineCalcJobWorkChain
from scatter_gather_calcjob_workchain import ScatterGatherCalcJobWorkChain


HOME = Path.home()

BASE_DIR = HOME / "wms_hpc_benchmark"

PIPELINE_SCRIPTS_DIR = BASE_DIR / "benchmark_scripts" / "pipeline"

SCATTER_SCRIPTS_DIR = BASE_DIR / "benchmark_scripts" / "scatter_gather"

RESULTS_DIR = BASE_DIR / "results_psql_exclusive" / "aiida"

SMOKE_RESULTS_DIR = BASE_DIR / "results_psql_exclusive" / "aiida_smoke"

CODE_LABEL = "aiida_python312@mogon-local"

THREAD_LIMITS = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "VECLIB_MAXIMUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
}

WORKLOADS = ["short", "medium", "long"]
CHUNKS = [1, 2, 4]

REPETITIONS = {
    "short": 5,
    "medium": 5,
    "long": 3,
}

REPEAT_FACTORS = {
    "short": 1,
    "medium": 13,
    "long": 60,
}

BASE_MATRIX_CHECKSUM = 4_522_800_285

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


def seconds(start_ns, end_ns):
    return (end_ns - start_ns) / 1_000_000_000


def format_value(value):
    if value == "":
        return ""

    return f"{value:.6f}"


def expected_checksum(workload):
    return BASE_MATRIX_CHECKSUM * REPEAT_FACTORS[workload]


def validate_pipeline_summary(summary, workload):
    expected = (
        "Summary of Pipeline computation\n"
        "-------------------------------\n"
        "count=71\n"
        "sum=1353\n"
        "mean=19.06\n"
        f"matrix_checksum={expected_checksum(workload)}\n"
    )

    if summary != expected:
        raise RuntimeError(
            f"Unerwartetes Pipeline-Ergebnis für {workload}:\n{summary}"
        )


def validate_scatter_summary(summary, workload, chunks):
    expected = (
        "Summary of Scatter-Gather computation\n"
        "-------------------------------------\n"
        f"chunks={chunks}\n"
        "total_count=71\n"
        "total_sum=1353\n"
        "global_mean=19.06\n"
        f"total_matrix_checksum={expected_checksum(workload)}\n"
    )

    if summary != expected:
        raise RuntimeError(
            "Unerwartetes Scatter-Gather-Ergebnis "
            f"für {workload}, chunks={chunks}:\n{summary}"
        )


def singlefile(scripts_dir, filename):
    path = scripts_dir / filename

    if not path.is_file():
        raise RuntimeError(f"Skript fehlt: {path}")

    return SinglefileData(file=path, filename=filename)


def add_common_scripts(builder, scripts_dir):
    builder.scripts.generate_input = singlefile(
        scripts_dir,
        "generate_input.py",
    )
    builder.scripts.preprocess = singlefile(
        scripts_dir,
        "preprocess.py",
    )
    builder.scripts.compute_short = singlefile(
        scripts_dir,
        "compute.py",
    )
    builder.scripts.compute_medium = singlefile(
        scripts_dir,
        "compute_medium.py",
    )
    builder.scripts.compute_long = singlefile(
        scripts_dir,
        "compute_long.py",
    )
    builder.scripts.postprocess = singlefile(
        scripts_dir,
        "postprocess.py",
    )
    builder.scripts.benchmark_timing = singlefile(
        scripts_dir,
        "benchmark_timing.py",
    )


def add_scatter_scripts(builder):
    add_common_scripts(builder, SCATTER_SCRIPTS_DIR)
    builder.scripts.split = singlefile(
        SCATTER_SCRIPTS_DIR,
        "split.py",
    )
    builder.scripts.aggregate = singlefile(
        SCATTER_SCRIPTS_DIR,
        "aggregate.py",
    )


def wait_for_finished(node):
    while not node.is_terminated:
        time.sleep(0.05)
        node = load_node(node.pk)

    return load_node(node.pk)


def parse_timing_text(content):
    values = {}

    for line in content.splitlines():
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key] = value

    required = {"task_name", "task_start_ns", "task_end_ns"}

    if not required.issubset(values):
        raise RuntimeError(
            f"Unvollständige Timing-Datei:\n{content}"
        )

    return {
        "task_name": values["task_name"],
        "task_start_ns": int(values["task_start_ns"]),
        "task_end_ns": int(values["task_end_ns"]),
    }


def get_child_by_label(node, label):
    matches = [
        child
        for child in node.called
        if child.label == label
    ]

    if len(matches) != 1:
        raise RuntimeError(
            f"Erwartet: genau ein Child mit Label '{label}', "
            f"gefunden: {len(matches)}"
        )

    return matches[0]


def get_child_timing(node, label):
    child = get_child_by_label(node, label)
    filename = f"timing_{label}.txt"

    content = child.outputs.retrieved.base.repository.get_object_content(
        filename
    )

    timing = parse_timing_text(content)

    if timing["task_name"] != label:
        raise RuntimeError(
            f"Timing-Datei {filename} enthält task_name="
            f"{timing['task_name']} statt {label}"
        )

    return timing


def run_aiida_pipeline(workload):
    builder = PipelineCalcJobWorkChain.get_builder()
    builder.code = load_code(CODE_LABEL)
    builder.workload = Str(workload)
    add_common_scripts(builder, PIPELINE_SCRIPTS_DIR)

    start_ns = time.time_ns()
    node = submit(builder)
    node = wait_for_finished(node)
    end_ns = time.time_ns()

    if not node.is_finished_ok:
        raise RuntimeError(
            f"AiiDA-Pipeline PK {node.pk} fehlgeschlagen: "
            f"{node.process_state} / Exit {node.exit_status}"
        )

    summary = node.outputs.summary_folder.base.repository.get_object_content(
        "summary.txt"
    )
    validate_pipeline_summary(summary, workload)

    timings = {
        "generate_input": get_child_timing(node, "generate_input"),
        "preprocess": get_child_timing(node, "preprocess"),
        "compute_1": get_child_timing(node, "compute_1"),
        "postprocess": get_child_timing(node, "postprocess"),
    }

    return {
        "makespan_s": seconds(start_ns, end_ns),
        "timings": timings,
        "node_pk": node.pk,
    }


def run_aiida_scatter(workload, chunks):
    builder = ScatterGatherCalcJobWorkChain.get_builder()
    builder.code = load_code(CODE_LABEL)
    builder.workload = Str(workload)
    builder.num_chunks = Int(chunks)
    add_scatter_scripts(builder)

    start_ns = time.time_ns()
    node = submit(builder)
    node = wait_for_finished(node)
    end_ns = time.time_ns()

    if not node.is_finished_ok:
        raise RuntimeError(
            f"AiiDA-Scatter-Gather PK {node.pk} fehlgeschlagen: "
            f"{node.process_state} / Exit {node.exit_status}"
        )

    summary = node.outputs.summary_folder.base.repository.get_object_content(
        "summary.txt"
    )
    validate_scatter_summary(summary, workload, chunks)

    timings = {
        "generate_input": get_child_timing(node, "generate_input"),
        "preprocess": get_child_timing(node, "preprocess"),
        "split": get_child_timing(node, "split"),
        "aggregate": get_child_timing(node, "aggregate"),
        "postprocess": get_child_timing(node, "postprocess"),
    }

    for index in range(1, chunks + 1):
        label = f"compute_{index}"
        timings[label] = get_child_timing(node, label)

    return {
        "makespan_s": seconds(start_ns, end_ns),
        "timings": timings,
        "node_pk": node.pk,
    }


def _run_reference_task(scripts_dir, work_dir, script_name, *arguments):
    environment = os.environ.copy()
    environment.update(THREAD_LIMITS)
    result = subprocess.run(
        ["python3", str(scripts_dir / script_name), *arguments],
        cwd=work_dir,
        env=environment,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(
            f"Referenz-Task fehlgeschlagen ({script_name}): {result.stderr}"
        )


def _reference_compute_phase(work_dir):
    starts = []
    ends = []
    for path in work_dir.glob("timing_compute_*.txt"):
        values = {}
        for line in path.read_text().splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
        starts.append(int(values["task_start_ns"]))
        ends.append(int(values["task_end_ns"]))
    if not starts:
        raise RuntimeError(f"Keine Compute-Timings in {work_dir}")
    return (max(ends) - min(starts)) / 1e9


def run_reference_execution(pattern, workload, chunks, work_dir):
    """Fuehrt die Referenz (reine Python-Kette) frisch aus.

    Identisch zum Vorgehen der StreamFlow-/Nextflow-Benchmarks, damit der
    Overhead in allen Systemen gegen dieselbe Referenzart gemessen wird.
    """
    compute_file = {
        "short": "compute.py",
        "medium": "compute_medium.py",
        "long": "compute_long.py",
    }[workload]

    scripts_dir = (
        PIPELINE_SCRIPTS_DIR if pattern == "pipeline" else SCATTER_SCRIPTS_DIR
    )

    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)

    start = time.perf_counter()
    _run_reference_task(scripts_dir, work_dir, "generate_input.py", "raw_input.txt")
    _run_reference_task(
        scripts_dir, work_dir, "preprocess.py", "raw_input.txt", "prepared_input.txt"
    )

    if pattern == "pipeline":
        _run_reference_task(
            scripts_dir, work_dir, compute_file, "prepared_input.txt", "result.txt"
        )
        _run_reference_task(
            scripts_dir, work_dir, "postprocess.py", "result.txt", "summary.txt"
        )
        makespan = time.perf_counter() - start
        return {"makespan_s": makespan, "compute_phase_s": ""}

    _run_reference_task(
        scripts_dir, work_dir, "split.py", "prepared_input.txt", str(chunks)
    )

    def compute(index):
        _run_reference_task(
            scripts_dir,
            work_dir,
            compute_file,
            f"chunk_{index}.txt",
            f"result_{index}.txt",
        )

    with ThreadPoolExecutor(max_workers=chunks) as executor:
        list(executor.map(compute, range(1, chunks + 1)))

    _run_reference_task(
        scripts_dir,
        work_dir,
        "aggregate.py",
        *[f"result_{index}.txt" for index in range(1, chunks + 1)],
        "aggregated_result.txt",
    )
    _run_reference_task(
        scripts_dir, work_dir, "postprocess.py", "aggregated_result.txt", "summary.txt"
    )
    makespan = time.perf_counter() - start
    compute_phase = _reference_compute_phase(work_dir)
    return {"makespan_s": makespan, "compute_phase_s": compute_phase}


def pipeline_row(workload, repetition, reference, aiida):
    timings = aiida["timings"]

    ref_makespan = reference["makespan_s"]
    wms_makespan = aiida["makespan_s"]
    overhead = wms_makespan - ref_makespan

    return {
        "system": "aiida",
        "pattern": "pipeline",
        "workload": workload,
        "chunks": 1,
        "repetition": repetition,
        "ref_makespan_s": ref_makespan,
        "wms_makespan_s": wms_makespan,
        "overhead_s": overhead,
        "overhead_pct": overhead / ref_makespan * 100,
        "ratio": wms_makespan / ref_makespan,
        "ref_compute_phase_s": "",
        "wms_compute_phase_s": "",
        "wms_task_span_s": seconds(
            timings["generate_input"]["task_start_ns"],
            timings["postprocess"]["task_end_ns"],
        ),
        "wms_gen_to_pre_s": seconds(
            timings["generate_input"]["task_end_ns"],
            timings["preprocess"]["task_start_ns"],
        ),
        "wms_pre_to_comp_s": seconds(
            timings["preprocess"]["task_end_ns"],
            timings["compute_1"]["task_start_ns"],
        ),
        "wms_comp_to_post_s": seconds(
            timings["compute_1"]["task_end_ns"],
            timings["postprocess"]["task_start_ns"],
        ),
        "wms_pre_to_split_s": "",
        "wms_split_to_comp_s": "",
        "wms_comp_to_agg_s": "",
        "wms_agg_to_post_s": "",
        "wms_start_spread_s": "",
    }


def scatter_row(workload, chunks, repetition, reference, aiida):
    timings = aiida["timings"]

    compute_timings = [
        timings[f"compute_{index}"]
        for index in range(1, chunks + 1)
    ]

    first_compute_start = min(
        item["task_start_ns"]
        for item in compute_timings
    )
    last_compute_start = max(
        item["task_start_ns"]
        for item in compute_timings
    )
    last_compute_end = max(
        item["task_end_ns"]
        for item in compute_timings
    )

    ref_makespan = reference["makespan_s"]
    wms_makespan = aiida["makespan_s"]
    overhead = wms_makespan - ref_makespan

    return {
        "system": "aiida",
        "pattern": "scatter_gather",
        "workload": workload,
        "chunks": chunks,
        "repetition": repetition,
        "ref_makespan_s": ref_makespan,
        "wms_makespan_s": wms_makespan,
        "overhead_s": overhead,
        "overhead_pct": overhead / ref_makespan * 100,
        "ratio": wms_makespan / ref_makespan,
        "ref_compute_phase_s": reference["compute_phase_s"],
        "wms_compute_phase_s": seconds(
            first_compute_start,
            last_compute_end,
        ),
        "wms_task_span_s": seconds(
            timings["generate_input"]["task_start_ns"],
            timings["postprocess"]["task_end_ns"],
        ),
        "wms_gen_to_pre_s": seconds(
            timings["generate_input"]["task_end_ns"],
            timings["preprocess"]["task_start_ns"],
        ),
        "wms_pre_to_comp_s": "",
        "wms_comp_to_post_s": "",
        "wms_pre_to_split_s": seconds(
            timings["preprocess"]["task_end_ns"],
            timings["split"]["task_start_ns"],
        ),
        "wms_split_to_comp_s": seconds(
            timings["split"]["task_end_ns"],
            first_compute_start,
        ),
        "wms_comp_to_agg_s": seconds(
            last_compute_end,
            timings["aggregate"]["task_start_ns"],
        ),
        "wms_agg_to_post_s": seconds(
            timings["aggregate"]["task_end_ns"],
            timings["postprocess"]["task_start_ns"],
        ),
        "wms_start_spread_s": seconds(
            first_compute_start,
            last_compute_start,
        ),
    }


def write_rows(rows, output, fields):
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    key: (
                        format_value(value)
                        if isinstance(value, float)
                        else value
                    )
                    for key, value in row.items()
                }
            )


def ordered_configuration_keys():
    keys = []

    for workload in WORKLOADS:
        keys.append(("pipeline", workload, 1))

    for workload in WORKLOADS:
        for chunks in CHUNKS:
            keys.append(("scatter_gather", workload, chunks))

    return keys


def create_summary_rows(rows):
    grouped = {}

    for row in rows:
        key = (
            row["system"],
            row["pattern"],
            row["workload"],
            row["chunks"],
        )
        grouped.setdefault(key, []).append(row)

    central_rows = []
    coordination_rows = []

    for pattern, workload, chunks in ordered_configuration_keys():
        key = ("aiida", pattern, workload, chunks)

        if key not in grouped:
            continue

        group = grouped[key]

        ref_makespan = statistics.median(
            float(row["ref_makespan_s"])
            for row in group
        )
        wms_makespan = statistics.median(
            float(row["wms_makespan_s"])
            for row in group
        )
        overhead = wms_makespan - ref_makespan

        if pattern == "pipeline":
            ref_compute_phase = ""
            wms_compute_phase = ""
        else:
            ref_compute_phase = statistics.median(
                float(row["ref_compute_phase_s"])
                for row in group
            )
            wms_compute_phase = statistics.median(
                float(row["wms_compute_phase_s"])
                for row in group
            )

        central_rows.append(
            {
                "system": "aiida",
                "pattern": pattern,
                "workload": workload,
                "chunks": chunks,
                "ref_makespan_s": ref_makespan,
                "wms_makespan_s": wms_makespan,
                "overhead_s": overhead,
                "overhead_pct": overhead / ref_makespan * 100,
                "ratio": wms_makespan / ref_makespan,
                "ref_compute_phase_s": ref_compute_phase,
                "wms_compute_phase_s": wms_compute_phase,
            }
        )

        coordination = {
            "system": "aiida",
            "pattern": pattern,
            "workload": workload,
            "chunks": chunks,
        }

        for field in COORDINATION_FIELDS[4:]:
            values = [
                float(row[field])
                for row in group
                if row[field] != ""
            ]
            coordination[field] = (
                statistics.median(values)
                if values
                else ""
            )

        coordination_rows.append(coordination)

    return central_rows, coordination_rows


def build_configurations(smoke):
    if smoke:
        return [
            ("pipeline", "short", 1, 1),
            ("scatter_gather", "short", 4, 1),
        ]

    configurations = []

    for workload in WORKLOADS:
        for repetition in range(1, REPETITIONS[workload] + 1):
            configurations.append(
                ("pipeline", workload, 1, repetition)
            )

    for workload in WORKLOADS:
        for chunks in CHUNKS:
            for repetition in range(1, REPETITIONS[workload] + 1):
                configurations.append(
                    ("scatter_gather", workload, chunks, repetition)
                )

    return configurations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke",
        action="store_true",
        help=(
            "Führt nur Pipeline short und Scatter-Gather short "
            "mit vier Chunks jeweils einmal aus."
        ),
    )
    args = parser.parse_args()

    if not PIPELINE_SCRIPTS_DIR.is_dir():
        raise RuntimeError(
            f"Pipeline-Skriptordner fehlt: {PIPELINE_SCRIPTS_DIR}"
        )

    if not SCATTER_SCRIPTS_DIR.is_dir():
        raise RuntimeError(
            f"Scatter-Skriptordner fehlt: {SCATTER_SCRIPTS_DIR}"
        )

    load_profile("psql_494992")

    # Referenz wird pro Lauf frisch gemessen (konsistent mit StreamFlow/Nextflow).
    output_dir = SMOKE_RESULTS_DIR if args.smoke else RESULTS_DIR

    if output_dir.exists():
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    configurations = build_configurations(args.smoke)
    rows = []

    for position, (pattern, workload, chunks, repetition) in enumerate(
        configurations,
        start=1,
    ):
        print(
            f"[{position}/{len(configurations)}] "
            f"{pattern} | workload={workload} | "
            f"chunks={chunks} | repetition={repetition}",
            flush=True,
        )

        reference_dir = (
            output_dir
            / "reference_runs"
            / f"{pattern}_{workload}_c{chunks}_r{repetition}"
        )
        reference = run_reference_execution(
            pattern, workload, chunks, reference_dir
        )

        if pattern == "pipeline":
            aiida = run_aiida_pipeline(workload)
            row = pipeline_row(
                workload,
                repetition,
                reference,
                aiida,
            )
        else:
            aiida = run_aiida_scatter(workload, chunks)
            row = scatter_row(
                workload,
                chunks,
                repetition,
                reference,
                aiida,
            )

        rows.append(row)

        write_rows(
            rows,
            output_dir / "aiida_raw_results.csv",
            RAW_FIELDS,
        )

        print(
            f"  reference={row['ref_makespan_s']:.3f}s | "
            f"aiida={row['wms_makespan_s']:.3f}s | "
            f"PK={aiida['node_pk']}",
            flush=True,
        )

    central_rows, coordination_rows = create_summary_rows(rows)

    write_rows(
        central_rows,
        output_dir / "aiida_central_results.csv",
        CENTRAL_FIELDS,
    )
    write_rows(
        coordination_rows,
        output_dir / "aiida_coordination_results.csv",
        COORDINATION_FIELDS,
    )

    print()
    print("Benchmark abgeschlossen.")
    print(f"Raw:          {output_dir / 'aiida_raw_results.csv'}")
    print(f"Central:      {output_dir / 'aiida_central_results.csv'}")
    print(f"Coordination: {output_dir / 'aiida_coordination_results.csv'}")


if __name__ == "__main__":
    main()
