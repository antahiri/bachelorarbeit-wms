#!/usr/bin/env python3

from aiida.engine import ToContext, WorkChain
from aiida.orm import Dict, FolderData, Int, List, SinglefileData, Str

from python_task_calcjob import PythonTaskCalcJob


COMPUTE_SCRIPT_KEYS = {
    "short": "compute_short",
    "medium": "compute_medium",
    "long": "compute_long",
}


class ScatterGatherCalcJobWorkChain(WorkChain):
    """Scatter-Gather mit externen Python-Tasks als AiiDA-CalcJobs."""

    @classmethod
    def define(cls, spec):
        super().define(spec)

        spec.input("code")
        spec.input("num_chunks", valid_type=Int)
        spec.input("workload", valid_type=Str)

        spec.input_namespace(
            "scripts",
            valid_type=SinglefileData,
            dynamic=True,
            help="Vorhandene Scatter-Gather-Benchmark-Skripte.",
        )

        spec.outline(
            cls.submit_generate,
            cls.submit_preprocess,
            cls.submit_split,
            cls.submit_compute,
            cls.submit_aggregate,
            cls.submit_postprocess,
            cls.results,
        )

        spec.output("raw_folder", valid_type=FolderData)
        spec.output("prepared_folder", valid_type=FolderData)
        spec.output("split_folder", valid_type=FolderData)
        spec.output("aggregate_folder", valid_type=FolderData)
        spec.output("summary_folder", valid_type=FolderData)

        spec.exit_code(
            400,
            "ERROR_TASK_FAILED",
            message="Ein Scatter-Gather-CalcJob ist fehlgeschlagen.",
        )

    def submit_task(
        self,
        label,
        script,
        arguments,
        retrieve_files,
        source_folders=None,
        mappings=None,
    ):
        inputs = {
            "code": self.inputs.code,
            "script": script,
            "timing_script": self.inputs.scripts.benchmark_timing,
            "arguments": List(list=arguments),
            "retrieve_files": List(list=retrieve_files),
            "metadata": {
                "label": label,
                "disable_cache": True,
                "options": {
                    "withmpi": False,
                    "resources": {
                        "num_machines": 1,
                        "num_mpiprocs_per_machine": 1,
                    },
                },
            },
        }

        if source_folders:
            inputs["source_folders"] = source_folders
            inputs["file_mappings"] = Dict(
                dict={"files": mappings or []}
            )

        return self.submit(PythonTaskCalcJob, **inputs)

    def submit_generate(self):
        future = self.submit_task(
            label="generate_input",
            script=self.inputs.scripts.generate_input,
            arguments=["raw_input.txt"],
            retrieve_files=[
                "raw_input.txt",
                "timing_generate_input.txt",
            ],
        )
        return ToContext(generate=future)

    def submit_preprocess(self):
        if not self.ctx.generate.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        future = self.submit_task(
            label="preprocess",
            script=self.inputs.scripts.preprocess,
            arguments=["raw_input.txt", "prepared_input.txt"],
            retrieve_files=[
                "prepared_input.txt",
                "timing_preprocess.txt",
            ],
            source_folders={
                "generate": self.ctx.generate.outputs.retrieved,
            },
            mappings=[
                {
                    "folder": "generate",
                    "source": "raw_input.txt",
                    "target": "raw_input.txt",
                }
            ],
        )
        return ToContext(preprocess=future)

    def submit_split(self):
        if not self.ctx.preprocess.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        future = self.submit_task(
            label="split",
            script=self.inputs.scripts.split,
            arguments=[
                "prepared_input.txt",
                str(self.inputs.num_chunks.value),
            ],
            retrieve_files=[
                *[
                    f"chunk_{index}.txt"
                    for index in range(
                        1,
                        self.inputs.num_chunks.value + 1,
                    )
                ],
                "timing_split.txt",
            ],
            source_folders={
                "preprocess": self.ctx.preprocess.outputs.retrieved,
            },
            mappings=[
                {
                    "folder": "preprocess",
                    "source": "prepared_input.txt",
                    "target": "prepared_input.txt",
                }
            ],
        )
        return ToContext(split=future)

    def submit_compute(self):
        if not self.ctx.split.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        workload = self.inputs.workload.value

        if workload not in COMPUTE_SCRIPT_KEYS:
            raise ValueError(f"Unbekannter Workload: {workload}")

        script = self.inputs.scripts[COMPUTE_SCRIPT_KEYS[workload]]
        futures = {}

        for index in range(1, self.inputs.num_chunks.value + 1):
            future = self.submit_task(
                label=f"compute_{index}",
                script=script,
                arguments=[
                    f"chunk_{index}.txt",
                    f"result_{index}.txt",
                ],
                retrieve_files=[
                    f"result_{index}.txt",
                    f"timing_compute_{index}.txt",
                ],
                source_folders={
                    "split": self.ctx.split.outputs.retrieved,
                },
                mappings=[
                    {
                        "folder": "split",
                        "source": f"chunk_{index}.txt",
                        "target": f"chunk_{index}.txt",
                    }
                ],
            )

            futures[f"compute_{index}"] = future

        return ToContext(**futures)

    def submit_aggregate(self):
        source_folders = {}
        mappings = []
        result_arguments = []

        for index in range(1, self.inputs.num_chunks.value + 1):
            child = self.ctx[f"compute_{index}"]

            if not child.is_finished_ok:
                return self.exit_codes.ERROR_TASK_FAILED

            folder_name = f"compute_{index}"
            result_name = f"result_{index}.txt"

            source_folders[folder_name] = child.outputs.retrieved
            mappings.append(
                {
                    "folder": folder_name,
                    "source": result_name,
                    "target": result_name,
                }
            )
            result_arguments.append(result_name)

        future = self.submit_task(
            label="aggregate",
            script=self.inputs.scripts.aggregate,
            arguments=[
                *result_arguments,
                "aggregated_result.txt",
            ],
            retrieve_files=[
                "aggregated_result.txt",
                "timing_aggregate.txt",
            ],
            source_folders=source_folders,
            mappings=mappings,
        )
        return ToContext(aggregate=future)

    def submit_postprocess(self):
        if not self.ctx.aggregate.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        future = self.submit_task(
            label="postprocess",
            script=self.inputs.scripts.postprocess,
            arguments=[
                "aggregated_result.txt",
                "summary.txt",
            ],
            retrieve_files=[
                "summary.txt",
                "timing_postprocess.txt",
            ],
            source_folders={
                "aggregate": self.ctx.aggregate.outputs.retrieved,
            },
            mappings=[
                {
                    "folder": "aggregate",
                    "source": "aggregated_result.txt",
                    "target": "aggregated_result.txt",
                }
            ],
        )
        return ToContext(postprocess=future)

    def results(self):
        if not self.ctx.postprocess.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        self.out("raw_folder", self.ctx.generate.outputs.retrieved)
        self.out("prepared_folder", self.ctx.preprocess.outputs.retrieved)
        self.out("split_folder", self.ctx.split.outputs.retrieved)
        self.out("aggregate_folder", self.ctx.aggregate.outputs.retrieved)
        self.out("summary_folder", self.ctx.postprocess.outputs.retrieved)
