# Auszug aus run_aiida_psql.py

## Messparameter (Zeilen 29-60)
```python
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
```

## Referenzmessung (Zeilen 362-461)
```python
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


```

## Systemmessung mit Warten auf Fertigstellung (Zeilen 217-224 und 284-360)
```python
def wait_for_finished(node):
    while not node.is_terminated:
        time.sleep(0.05)
        node = load_node(node.pk)

    return load_node(node.pk)



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

```

## Berechnung der Koordinationsmetriken (Zeilen 506-574)
```python
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
```
