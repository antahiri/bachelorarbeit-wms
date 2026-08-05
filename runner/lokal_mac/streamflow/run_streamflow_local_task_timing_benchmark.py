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

BASE_DIR = Path.home() / "wms_benchmark_reference"
PIPELINE_SOURCE = Path.home() / "streamflow_pipeline_benchmark_local"
SCATTER_SOURCE = Path.home() / "streamflow_scatter_gather_benchmark_local"
PIPELINE_REFERENCE_SCRIPTS = (
    Path.home() / "nextflow_pipeline_test" / "benchmark_scripts"
)
SCATTER_REFERENCE_SCRIPTS = (
    Path.home() / "nextflow_scatter_gather_test" / "benchmark_scripts"
)
RESULT_ROOT = BASE_DIR / "results" / "streamflow_local_task_timing"
RUN_ROOT = BASE_DIR / "run_data" / "streamflow_local" / "task_timing_benchmark"

STREAMFLOW = shutil.which("streamflow") or str(
    Path.home() / "miniforge3" / "envs" / "streamflow-local" / "bin" / "streamflow"
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

METADATA_FIELDS = [
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
]

TASK_FIELDS = [
    "task_name",
    "task_start_ns",
    "task_end_ns",
    "task_duration_s",
    "timing_file",
]

STANDALONE_TIMING_HELPER = """\
from pathlib import Path


def write_timing(task_name: str, start_ns: int, end_ns: int) -> None:
    Path(f"timing_{task_name}.txt").write_text(
        f"task_name={task_name}\\n"
        f"task_start_ns={start_ns}\\n"
        f"task_end_ns={end_ns}\\n"
    )
"""


PIPELINE_CWL = """\
cwlVersion: v1.2
class: Workflow

inputs:
  generate_script: File
  preprocess_script: File
  compute_script: File
  postprocess_script: File
  timing_helper: File

outputs:
  final_summary:
    type: File
    outputSource: postprocess/summary_file
  timing_generate_input:
    type: File
    outputSource: generate_input/timing_file
  timing_preprocess:
    type: File
    outputSource: preprocess/timing_file
  timing_compute_1:
    type: File
    outputSource: compute/timing_file
  timing_postprocess:
    type: File
    outputSource: postprocess/timing_file

steps:
  generate_input:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        output_name:
          type: string
          default: raw_input.txt
          inputBinding: {position: 2}
      outputs:
        raw_file: {type: File, outputBinding: {glob: raw_input.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_generate_input.txt}}
    in:
      script: generate_script
      timing_helper: timing_helper
    out: [raw_file, timing_file]

  preprocess:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        raw_file: {type: File, inputBinding: {position: 2}}
        output_name:
          type: string
          default: prepared_input.txt
          inputBinding: {position: 3}
      outputs:
        prepared_file: {type: File, outputBinding: {glob: prepared_input.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_preprocess.txt}}
    in:
      script: preprocess_script
      timing_helper: timing_helper
      raw_file: generate_input/raw_file
    out: [prepared_file, timing_file]

  compute:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        prepared_file: {type: File, inputBinding: {position: 2}}
        output_name:
          type: string
          default: result.txt
          inputBinding: {position: 3}
      outputs:
        result_file: {type: File, outputBinding: {glob: result.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_compute_1.txt}}
    in:
      script: compute_script
      timing_helper: timing_helper
      prepared_file: preprocess/prepared_file
    out: [result_file, timing_file]

  postprocess:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        result_file: {type: File, inputBinding: {position: 2}}
        output_name:
          type: string
          default: summary.txt
          inputBinding: {position: 3}
      outputs:
        summary_file: {type: File, outputBinding: {glob: summary.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_postprocess.txt}}
    in:
      script: postprocess_script
      timing_helper: timing_helper
      result_file: compute/result_file
    out: [summary_file, timing_file]
"""


SCATTER_CWL = """\
cwlVersion: v1.2
class: Workflow

requirements:
  ScatterFeatureRequirement: {}
  InlineJavascriptRequirement: {}

inputs:
  generate_script: File
  preprocess_script: File
  split_script: File
  compute_script: File
  aggregate_script: File
  postprocess_script: File
  timing_helper: File
  number_of_chunks: int
  output_names:
    type: {type: array, items: string}

outputs:
  final_summary:
    type: File
    outputSource: postprocess/summary_file
  compute_results:
    type: {type: array, items: File}
    outputSource: compute/result_file
  timing_generate_input:
    type: File
    outputSource: generate_input/timing_file
  timing_preprocess:
    type: File
    outputSource: preprocess/timing_file
  timing_split:
    type: File
    outputSource: split/timing_file
  timing_compute:
    type: {type: array, items: File}
    outputSource: compute/timing_file
  timing_aggregate:
    type: File
    outputSource: aggregate/timing_file
  timing_postprocess:
    type: File
    outputSource: postprocess/timing_file

steps:
  generate_input:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        output_name:
          type: string
          default: raw_input.txt
          inputBinding: {position: 2}
      outputs:
        raw_file: {type: File, outputBinding: {glob: raw_input.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_generate_input.txt}}
    in:
      script: generate_script
      timing_helper: timing_helper
    out: [raw_file, timing_file]

  preprocess:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        raw_file: {type: File, inputBinding: {position: 2}}
        output_name:
          type: string
          default: prepared_input.txt
          inputBinding: {position: 3}
      outputs:
        prepared_file: {type: File, outputBinding: {glob: prepared_input.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_preprocess.txt}}
    in:
      script: preprocess_script
      timing_helper: timing_helper
      raw_file: generate_input/raw_file
    out: [prepared_file, timing_file]

  split:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        prepared_file: {type: File, inputBinding: {position: 2}}
        number_of_chunks: {type: int, inputBinding: {position: 3}}
      outputs:
        chunk_files:
          type: {type: array, items: File}
          outputBinding: {glob: "chunk_*.txt"}
        timing_file: {type: File, outputBinding: {glob: timing_split.txt}}
    in:
      script: split_script
      timing_helper: timing_helper
      prepared_file: preprocess/prepared_file
      number_of_chunks: number_of_chunks
    out: [chunk_files, timing_file]

  compute:
    run:
      class: CommandLineTool
      requirements:
        InlineJavascriptRequirement: {}
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        chunk_file: {type: File, inputBinding: {position: 2}}
        output_name: {type: string, inputBinding: {position: 3}}
      outputs:
        result_file: {type: File, outputBinding: {glob: "$(inputs.output_name)"}}
        timing_file: {type: File, outputBinding: {glob: "timing_compute_*.txt"}}
    in:
      script: compute_script
      timing_helper: timing_helper
      chunk_file: split/chunk_files
      output_name: output_names
    scatter: [chunk_file, output_name]
    scatterMethod: dotproduct
    out: [result_file, timing_file]

  aggregate:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        result_files:
          type: {type: array, items: File}
          inputBinding: {position: 2}
        output_name:
          type: string
          default: aggregated_result.txt
          inputBinding: {position: 3}
      outputs:
        aggregated_file: {type: File, outputBinding: {glob: aggregated_result.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_aggregate.txt}}
    in:
      script: aggregate_script
      timing_helper: timing_helper
      result_files: compute/result_file
    out: [aggregated_file, timing_file]

  postprocess:
    run:
      class: CommandLineTool
      baseCommand: python3
      inputs:
        script: {type: File, inputBinding: {position: 1}}
        timing_helper: File
        aggregated_file: {type: File, inputBinding: {position: 2}}
        output_name:
          type: string
          default: summary.txt
          inputBinding: {position: 3}
      outputs:
        summary_file: {type: File, outputBinding: {glob: summary.txt}}
        timing_file: {type: File, outputBinding: {glob: timing_postprocess.txt}}
    in:
      script: postprocess_script
      timing_helper: timing_helper
      aggregated_file: aggregate/aggregated_file
    out: [summary_file, timing_file]
"""


STREAMFLOW_YML = """\
version: v1.0

workflows:
  local-task-timing-benchmark:
    type: cwl
    config:
      file: workflow.cwl
      settings: config.yml
    bindings:
      - step: /
        target:
          deployment: local-python

deployments:
  local-python:
    type: local
    config: {}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lokaler StreamFlow-Task-Timing-Benchmark"
    )
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("smoke", "full", "all"),
        default="all",
        help="Smoke-Tests, vollständiger Benchmark oder beides (Standard: all)",
    )
    return parser.parse_args()


def run_command(command: list[str], cwd: Path) -> None:
    environment = os.environ.copy()
    environment.update(THREAD_LIMITS)
    result = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(
            f"Befehl fehlgeschlagen ({' '.join(command)}):\n{result.stderr}"
        )


def write_csv_atomic(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        file.flush()
    temporary.replace(path)


def write_project_config(project_dir: Path, pattern: str, chunks: int) -> None:
    common = """\
generate_script: {class: File, path: scripts/generate_input.py}
preprocess_script: {class: File, path: scripts/preprocess.py}
compute_script: {class: File, path: scripts/compute.py}
postprocess_script: {class: File, path: scripts/postprocess.py}
timing_helper: {class: File, path: scripts/benchmark_timing.py}
"""
    if pattern == "pipeline":
        config = common
    else:
        output_names = "\n".join(
            f"  - result_{index}.txt" for index in range(1, chunks + 1)
        )
        config = (
            common
            + """\
split_script: {class: File, path: scripts/split.py}
aggregate_script: {class: File, path: scripts/aggregate.py}
"""
            + f"number_of_chunks: {chunks}\n"
            + f"output_names:\n{output_names}\n"
        )
    (project_dir / "config.yml").write_text(config)


def prepare_local_sources() -> None:
    """Aktualisiert ausschließlich die beiden vorgesehenen lokalen Kopien."""
    projects = (
        (PIPELINE_SOURCE, PIPELINE_REFERENCE_SCRIPTS, "pipeline"),
        (SCATTER_SOURCE, SCATTER_REFERENCE_SCRIPTS, "scatter_gather"),
    )
    for project, reference, pattern in projects:
        scripts = project / "scripts"
        scripts.mkdir(parents=True, exist_ok=True)
        names = [
            "benchmark_timing.py",
            "generate_input.py",
            "preprocess.py",
            "compute.py",
            "compute_medium.py",
            "compute_long.py",
            "postprocess.py",
        ]
        if pattern == "scatter_gather":
            names.extend(["split.py", "aggregate.py"])
        for name in names:
            shutil.copy2(reference / name, scripts / name)
        for script in scripts.glob("*.py"):
            if script.name == "benchmark_timing.py":
                continue
            content = script.read_text()
            timing_import = "from benchmark_timing import write_timing"
            if timing_import not in content:
                raise RuntimeError(f"Timing-Import fehlt im Fachskript: {script}")
            script.write_text(
                content.replace(timing_import, STANDALONE_TIMING_HELPER, 1)
            )
        (project / "workflow.cwl").write_text(
            PIPELINE_CWL if pattern == "pipeline" else SCATTER_CWL
        )
        (project / "streamflow.yml").write_text(STREAMFLOW_YML)
        write_project_config(
            project, pattern, 1 if pattern == "pipeline" else 4
        )


def create_project(
    project_dir: Path, pattern: str, workload: str, chunks: int
) -> None:
    if project_dir.exists():
        shutil.rmtree(project_dir)
    scripts_dir = project_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    source = PIPELINE_SOURCE if pattern == "pipeline" else SCATTER_SOURCE
    source_scripts = source / "scripts"
    script_names = ["benchmark_timing.py", "generate_input.py", "preprocess.py"]
    if pattern == "scatter_gather":
        script_names.extend(["split.py", "aggregate.py"])
    script_names.append("postprocess.py")
    for name in script_names:
        shutil.copy2(source_scripts / name, scripts_dir / name)
    shutil.copy2(
        source_scripts / WORKLOADS[workload]["compute_file"],
        scripts_dir / "compute.py",
    )
    (project_dir / "workflow.cwl").write_text(
        PIPELINE_CWL if pattern == "pipeline" else SCATTER_CWL
    )
    (project_dir / "streamflow.yml").write_text(STREAMFLOW_YML)
    write_project_config(project_dir, pattern, chunks)


def read_key_values(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def task_sort_key(row: dict) -> tuple[int, int]:
    name = row["task_name"]
    fixed = {
        "generate_input": (0, 0),
        "preprocess": (1, 0),
        "split": (2, 0),
        "aggregate": (4, 0),
        "postprocess": (5, 0),
    }
    if name.startswith("compute_"):
        suffix = name.removeprefix("compute_")
        return (3, int(suffix) if suffix.isdigit() else 999)
    return fixed.get(name, (99, 0))


def read_timings(root: Path, pattern: str, chunks: int) -> list[dict]:
    files = list(root.rglob("timing_*.txt"))
    timings = []
    for path in files:
        values = read_key_values(path)
        if not {"task_name", "task_start_ns", "task_end_ns"} <= values.keys():
            raise RuntimeError(f"Ungültige Timing-Datei: {path}")
        start = int(values["task_start_ns"])
        end = int(values["task_end_ns"])
        if end < start:
            raise RuntimeError(f"Negatives Task-Intervall: {path}")
        timings.append(
            {
                "task_name": values["task_name"],
                "task_start_ns": start,
                "task_end_ns": end,
                "task_duration_s": f"{(end - start) / 1e9:.9f}",
                "timing_file": str(path),
            }
        )
    expected = {"generate_input", "preprocess", "postprocess", "compute_1"}
    if pattern == "scatter_gather":
        expected |= {"split", "aggregate"}
        expected |= {f"compute_{index}" for index in range(1, chunks + 1)}
    names = [row["task_name"] for row in timings]
    duplicates = {name for name in names if names.count(name) > 1}
    missing = expected - set(names)
    unexpected = set(names) - expected
    if duplicates or missing or unexpected:
        raise RuntimeError(
            f"Timing-Dateien fehlerhaft in {root}: "
            f"fehlend={sorted(missing)}, doppelt={sorted(duplicates)}, "
            f"unerwartet={sorted(unexpected)}"
        )
    return sorted(timings, key=task_sort_key)


def seconds(start_ns: int, end_ns: int) -> float:
    return (end_ns - start_ns) / 1e9


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


def max_compute_concurrency(timings: list[dict]) -> int:
    events = []
    for row in timings:
        if row["task_name"].startswith("compute_"):
            events.extend(
                [
                    (row["task_start_ns"], 1),
                    (row["task_end_ns"], -1),
                ]
            )
    current = maximum = 0
    for _, delta in sorted(events, key=lambda event: (event[0], event[1])):
        current += delta
        maximum = max(maximum, current)
    return maximum


def find_single(root: Path, name: str) -> Path:
    matches = list(root.rglob(name))
    if len(matches) != 1:
        raise RuntimeError(
            f"Genau eine {name} erwartet in {root}, gefunden: {matches}"
        )
    return matches[0]


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
    return time.perf_counter() - start


def format_number(value: float | str) -> str:
    return value if value == "" else f"{float(value):.9f}"


def execute_repetition(
    root: Path,
    pattern: str,
    workload: str,
    chunks: int,
    repetition: int,
) -> dict:
    project = root / "project"
    reference_dir = root / "reference"
    wms_dir = root / "streamflow_output"
    create_project(project, pattern, workload, chunks)
    if pattern == "pipeline":
        ref_makespan = run_pipeline_reference(project, reference_dir)
    else:
        ref_makespan = run_scatter_reference(project, reference_dir, chunks)
    reference_timings = read_timings(reference_dir, pattern, chunks)
    write_csv_atomic(
        root / "reference_task_timings.csv",
        TASK_FIELDS,
        reference_timings,
    )
    wms_makespan = run_streamflow(project, wms_dir)
    summary = find_single(wms_dir, "summary.txt")
    if summary.read_text() != (reference_dir / "summary.txt").read_text():
        raise RuntimeError("StreamFlow- und Referenzergebnis unterscheiden sich.")
    wms_timings = read_timings(wms_dir, pattern, chunks)
    write_csv_atomic(
        root / "streamflow_task_timings.csv",
        TASK_FIELDS,
        wms_timings,
    )
    ref_metrics = calculate_metrics(reference_timings, pattern)
    wms_metrics = calculate_metrics(wms_timings, pattern)
    overhead = wms_makespan - ref_makespan
    row = {
        "system": "streamflow",
        "pattern": pattern,
        "workload": workload,
        "chunks": chunks,
        "repetition": repetition,
        "ref_makespan_s": format_number(ref_makespan),
        "wms_makespan_s": format_number(wms_makespan),
        "overhead_s": format_number(overhead),
        "overhead_pct": format_number(overhead / ref_makespan * 100),
        "ratio": format_number(wms_makespan / ref_makespan),
        "ref_compute_phase_s": format_number(ref_metrics["compute_phase_s"]),
        "wms_compute_phase_s": format_number(wms_metrics["compute_phase_s"]),
        "wms_task_span_s": format_number(wms_metrics["task_span_s"]),
        "wms_gen_to_pre_s": format_number(wms_metrics["gen_to_pre_s"]),
        "wms_pre_to_comp_s": format_number(wms_metrics["pre_to_comp_s"]),
        "wms_comp_to_post_s": format_number(wms_metrics["comp_to_post_s"]),
        "wms_pre_to_split_s": format_number(wms_metrics["pre_to_split_s"]),
        "wms_split_to_comp_s": format_number(wms_metrics["split_to_comp_s"]),
        "wms_comp_to_agg_s": format_number(wms_metrics["comp_to_agg_s"]),
        "wms_agg_to_post_s": format_number(wms_metrics["agg_to_post_s"]),
        "wms_start_spread_s": format_number(wms_metrics["start_spread_s"]),
    }
    write_csv_atomic(root / "run_metadata.csv", METADATA_FIELDS, [row])
    if pattern == "scatter_gather" and float(row["wms_start_spread_s"]) < 0:
        raise RuntimeError("Negativer Compute-Start-Spread.")
    row["_wms_max_concurrency"] = max_compute_concurrency(wms_timings)
    return row


def read_completed_rows() -> list[dict]:
    rows = []
    for metadata in RUN_ROOT.glob(
        "full/*/*/chunks_*/rep_*/run_metadata.csv"
    ):
        with metadata.open(newline="") as file:
            metadata_rows = list(csv.DictReader(file))
        if len(metadata_rows) != 1:
            raise RuntimeError(f"Ungültige Metadaten: {metadata}")
        row = metadata_rows[0]
        task_csv = metadata.parent / "streamflow_task_timings.csv"
        with task_csv.open(newline="") as file:
            timings = list(csv.DictReader(file))
        metrics = calculate_metrics(
            [
                {
                    **timing,
                    "task_start_ns": int(timing["task_start_ns"]),
                    "task_end_ns": int(timing["task_end_ns"]),
                }
                for timing in timings
            ],
            row["pattern"],
        )
        row.update(
            {
                "ref_compute_phase_s": (
                    ""
                    if row["pattern"] == "pipeline"
                    else read_reference_compute_phase(metadata.parent)
                ),
                "wms_compute_phase_s": format_number(metrics["compute_phase_s"]),
                "wms_task_span_s": format_number(metrics["task_span_s"]),
                "wms_gen_to_pre_s": format_number(metrics["gen_to_pre_s"]),
                "wms_pre_to_comp_s": format_number(metrics["pre_to_comp_s"]),
                "wms_comp_to_post_s": format_number(metrics["comp_to_post_s"]),
                "wms_pre_to_split_s": format_number(metrics["pre_to_split_s"]),
                "wms_split_to_comp_s": format_number(metrics["split_to_comp_s"]),
                "wms_comp_to_agg_s": format_number(metrics["comp_to_agg_s"]),
                "wms_agg_to_post_s": format_number(metrics["agg_to_post_s"]),
                "wms_start_spread_s": format_number(metrics["start_spread_s"]),
            }
        )
        rows.append(row)
    return sorted(
        rows,
        key=lambda row: (
            0 if row["pattern"] == "pipeline" else 1,
            tuple(WORKLOADS).index(row["workload"]),
            int(row["chunks"]),
            int(row["repetition"]),
        ),
    )


def read_reference_compute_phase(root: Path) -> str:
    with (root / "reference_task_timings.csv").open(newline="") as file:
        timings = list(csv.DictReader(file))
    compute_rows = [
        row for row in timings if row["task_name"].startswith("compute_")
    ]
    return format_number(
        seconds(
            min(int(row["task_start_ns"]) for row in compute_rows),
            max(int(row["task_end_ns"]) for row in compute_rows),
        )
    )


def median_field(rows: list[dict], field: str) -> str:
    values = [
        float(row[field]) for row in rows if row.get(field) not in ("", None)
    ]
    return "" if not values else f"{median(values):.9f}"


def rebuild_result_tables() -> None:
    raw_rows = read_completed_rows()
    write_csv_atomic(
        RESULT_ROOT / "streamflow_local_raw_results.csv", RAW_FIELDS, raw_rows
    )
    grouped: dict[tuple[str, str, int], list[dict]] = {}
    for row in raw_rows:
        key = (row["pattern"], row["workload"], int(row["chunks"]))
        grouped.setdefault(key, []).append(row)
    central_rows = []
    coordination_rows = []
    for (pattern, workload, chunks), rows in grouped.items():
        identity = {
            "system": "streamflow",
            "pattern": pattern,
            "workload": workload,
            "chunks": chunks,
        }
        reference_median = float(median_field(rows, "ref_makespan_s"))
        wms_median = float(median_field(rows, "wms_makespan_s"))
        overhead = wms_median - reference_median
        central_rows.append(
            {
                **identity,
                "ref_makespan_s": format_number(reference_median),
                "wms_makespan_s": format_number(wms_median),
                "overhead_s": format_number(overhead),
                "overhead_pct": format_number(
                    overhead / reference_median * 100
                ),
                "ratio": format_number(wms_median / reference_median),
                "ref_compute_phase_s": (
                    ""
                    if pattern == "pipeline"
                    else median_field(rows, "ref_compute_phase_s")
                ),
                "wms_compute_phase_s": (
                    ""
                    if pattern == "pipeline"
                    else median_field(rows, "wms_compute_phase_s")
                ),
            }
        )
        coordination_rows.append(
            {
                **identity,
                **{
                    field: median_field(rows, field)
                    for field in COORDINATION_FIELDS[4:]
                },
            }
        )
    write_csv_atomic(
        RESULT_ROOT / "streamflow_local_central_results.csv",
        CENTRAL_FIELDS,
        central_rows,
    )
    write_csv_atomic(
        RESULT_ROOT / "streamflow_local_coordination_results.csv",
        COORDINATION_FIELDS,
        coordination_rows,
    )


def smoke_tests() -> None:
    print("\n=== Smoke-Tests ===")
    smoke_root = RUN_ROOT / "smoke_tests"
    cases = (("pipeline", 1), ("scatter_gather", 4))
    for pattern, chunks in cases:
        run_root = smoke_root / pattern / "short" / f"chunks_{chunks}" / "rep_1"
        if run_root.exists():
            shutil.rmtree(run_root)
        row = execute_repetition(run_root, pattern, "short", chunks, 1)
        concurrency = row.pop("_wms_max_concurrency")
        if pattern == "scatter_gather" and concurrency < 2:
            raise RuntimeError(
                f"Kein plausibler Compute-Overlap: max_concurrency={concurrency}"
            )
        if pattern == "scatter_gather":
            with (run_root / "streamflow_task_timings.csv").open(
                newline=""
            ) as file:
                timings = list(csv.DictReader(file))
            compute_durations = [
                float(timing["task_duration_s"])
                for timing in timings
                if timing["task_name"].startswith("compute_")
            ]
            if float(row["wms_compute_phase_s"]) >= sum(compute_durations):
                raise RuntimeError(
                    "Scatter-Compute-Tasks überlappen nicht ausreichend."
                )
            if float(row["wms_start_spread_s"]) > 0.5:
                raise RuntimeError(
                    "Unplausibel großer lokaler Compute-Start-Spread."
                )
        print(
            f"OK {pattern}, short, chunks={chunks}: "
            f"ref={row['ref_makespan_s']} s, "
            f"StreamFlow={row['wms_makespan_s']} s, "
            f"compute_start_spread={row['wms_start_spread_s'] or 'n/a'} s, "
            f"max_compute_concurrency={concurrency}"
        )


def full_benchmark() -> None:
    print("\n=== Vollständiger Benchmark ===")
    configurations = [
        ("pipeline", workload, 1) for workload in WORKLOADS
    ] + [
        ("scatter_gather", workload, chunks)
        for workload in WORKLOADS
        for chunks in CHUNK_COUNTS
    ]
    completed = {
        (
            row["pattern"],
            row["workload"],
            int(row["chunks"]),
            int(row["repetition"]),
        )
        for row in read_completed_rows()
    }
    for pattern, workload, chunks in configurations:
        repetitions = WORKLOADS[workload]["repetitions"]
        for repetition in range(1, repetitions + 1):
            key = (pattern, workload, chunks, repetition)
            if key in completed:
                print(f"Überspringe vorhandenen Lauf: {key}")
                continue
            run_root = (
                RUN_ROOT
                / "full"
                / pattern
                / workload
                / f"chunks_{chunks}"
                / f"rep_{repetition}"
            )
            if run_root.exists():
                shutil.rmtree(run_root)
            row = execute_repetition(
                run_root, pattern, workload, chunks, repetition
            )
            row.pop("_wms_max_concurrency")
            rebuild_result_tables()
            print(
                f"OK {pattern} | {workload} | chunks={chunks} | "
                f"{repetition}/{repetitions}: "
                f"ref={row['ref_makespan_s']} s | "
                f"StreamFlow={row['wms_makespan_s']} s | "
                f"overhead={row['overhead_s']} s"
            )
    rebuild_result_tables()


def validate_final_results() -> None:
    rows = read_completed_rows()
    expected = {
        (pattern, workload, chunks): WORKLOADS[workload]["repetitions"]
        for pattern, workload, chunks in (
            [("pipeline", workload, 1) for workload in WORKLOADS]
            + [
                ("scatter_gather", workload, chunks)
                for workload in WORKLOADS
                for chunks in CHUNK_COUNTS
            ]
        )
    }
    actual = {}
    for row in rows:
        key = (row["pattern"], row["workload"], int(row["chunks"]))
        actual[key] = actual.get(key, 0) + 1
    if actual != expected:
        raise RuntimeError(
            f"Konfigurationen/Wiederholungen unvollständig: {actual}"
        )
    expected_raw = sum(expected.values())
    with (RESULT_ROOT / "streamflow_local_central_results.csv").open(
        newline=""
    ) as file:
        central_rows = list(csv.DictReader(file))
    central_count = len(central_rows)
    with (RESULT_ROOT / "streamflow_local_coordination_results.csv").open() as file:
        coordination_count = sum(1 for _ in csv.DictReader(file))
    if (len(rows), central_count, coordination_count) != (expected_raw, 12, 12):
        raise RuntimeError(
            "Unerwartete Zeilenzahlen: "
            f"raw={len(rows)}, central={central_count}, "
            f"coordination={coordination_count}"
        )
    for row in central_rows:
        reference = float(row["ref_makespan_s"])
        wms = float(row["wms_makespan_s"])
        overhead = wms - reference
        if abs(float(row["overhead_s"]) - overhead) > 1e-8:
            raise RuntimeError(f"Falscher zentraler Overhead: {row}")
        if abs(
            float(row["overhead_pct"]) - overhead / reference * 100
        ) > 1e-6:
            raise RuntimeError(f"Falscher zentraler Overhead-Prozentwert: {row}")
        if abs(float(row["ratio"]) - wms / reference) > 1e-8:
            raise RuntimeError(f"Falsches zentrales Verhältnis: {row}")
        if row["pattern"] == "pipeline" and (
            row["ref_compute_phase_s"] or row["wms_compute_phase_s"]
        ):
            raise RuntimeError(f"Pipeline-Compute-Phase nicht leer: {row}")
    print(
        f"\nAbschlussprüfung OK: raw={len(rows)}, "
        f"central={central_count}, coordination={coordination_count}"
    )


def verify_environment() -> None:
    if not Path(STREAMFLOW).is_file():
        raise RuntimeError(f"StreamFlow nicht gefunden: {STREAMFLOW}")
    if "streamflow-local" not in str(STREAMFLOW):
        raise RuntimeError(
            f"Falsche StreamFlow-Umgebung aktiv: {STREAMFLOW}"
        )
    version = subprocess.run(
        [STREAMFLOW, "version"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, **THREAD_LIMITS},
    )
    output = version.stdout + version.stderr
    if version.returncode or "0.2.0rc2" not in output:
        raise RuntimeError(f"Unerwartete StreamFlow-Version: {output}")
    import streamflow

    if streamflow.__version__ != "0.2.0rc2":
        raise RuntimeError(
            f"Unerwartete Python-Paketversion: {streamflow.__version__}"
        )
    if "docker" in STREAMFLOW_YML.lower():
        raise RuntimeError("Docker ist in der lokalen Konfiguration enthalten.")
    os.environ.update(THREAD_LIMITS)
    print(
        f"Umgebung OK: StreamFlow {streamflow.__version__}, "
        f"lokaler Connector, Python={PYTHON}"
    )


def main() -> None:
    args = parse_args()
    verify_environment()
    missing_sources = [
        path
        for path in (PIPELINE_SOURCE, SCATTER_SOURCE)
        if not path.is_dir()
    ]
    if missing_sources:
        raise RuntimeError(f"Fachskripte nicht gefunden: {missing_sources}")
    prepare_local_sources()
    if args.mode in ("smoke", "all"):
        smoke_tests()
    if args.mode in ("full", "all"):
        full_benchmark()
        validate_final_results()
        print(f"Ergebnisse: {RESULT_ROOT}")
        print(f"Laufdaten: {RUN_ROOT}")


if __name__ == "__main__":
    main()
