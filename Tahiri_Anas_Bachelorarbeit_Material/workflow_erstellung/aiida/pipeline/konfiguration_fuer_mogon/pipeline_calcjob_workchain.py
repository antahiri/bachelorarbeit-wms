#!/usr/bin/env python3

from aiida.engine import ToContext, WorkChain
from aiida.orm import Dict, FolderData, List, SinglefileData, Str

from python_task_calcjob import PythonTaskCalcJob


COMPUTE_SCRIPTS = {
    "short": "compute.py",
    "medium": "compute_medium.py",
    "long": "compute_long.py",
}


class PipelineCalcJobWorkChain(WorkChain):
    """Pipeline mit externen Python-Tasks als AiiDA-CalcJobs."""

    @classmethod
    def define(cls, spec):
        super().define(spec)

        spec.input("code")
        spec.input("workload", valid_type=Str)

        spec.input_namespace(
            "scripts",
            valid_type=SinglefileData,
            dynamic=True,
            help="Vorhandene Benchmark-Skripte.",
        )

        spec.outline(
            cls.submit_generate,
            cls.submit_preprocess,
            cls.submit_compute,
            cls.submit_postprocess,
            cls.results,
        )

        spec.output("raw_folder", valid_type=FolderData)
        spec.output("prepared_folder", valid_type=FolderData)
        spec.output("result_folder", valid_type=FolderData)
        spec.output("summary_folder", valid_type=FolderData)

        spec.exit_code(
            400,
            "ERROR_TASK_FAILED",
            message="Ein Pipeline-CalcJob ist fehlgeschlagen.",
        )

    def submit_task(
        self,
        label,
        script,
        arguments,
        retrieve_files,
        source_folder=None,
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
                    "queue_name": "ki-smallcpu",
                    "account": "ki-mawahpc",
                    "max_wallclock_seconds": 600,
		    "max_memory_kb": 1048576,
                    "resources": {
                        "num_machines": 1,
                        "num_mpiprocs_per_machine": 1,
                    },
                },
            },
        }

        if source_folder is not None:
            inputs["source_folders"] = {"previous": source_folder}
            inputs["file_mappings"] = Dict(
                dict={"files": mappings}
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
            source_folder=self.ctx.generate.outputs.retrieved,
            mappings=[
                {
                    "folder": "previous",
                    "source": "raw_input.txt",
                    "target": "raw_input.txt",
                }
            ],
        )
        return ToContext(preprocess=future)

    def submit_compute(self):
        if not self.ctx.preprocess.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        workload = self.inputs.workload.value

        if workload not in COMPUTE_SCRIPTS:
            raise ValueError(f"Unbekannter Workload: {workload}")

        script_key = {
            "short": "compute_short",
            "medium": "compute_medium",
            "long": "compute_long",
        }[workload]

        future = self.submit_task(
            label="compute_1",
            script=self.inputs.scripts[script_key],
            arguments=["prepared_input.txt", "result.txt"],
            retrieve_files=[
                "result.txt",
                "timing_compute_1.txt",
            ],
            source_folder=self.ctx.preprocess.outputs.retrieved,
            mappings=[
                {
                    "folder": "previous",
                    "source": "prepared_input.txt",
                    "target": "prepared_input.txt",
                }
            ],
        )
        return ToContext(compute=future)

    def submit_postprocess(self):
        if not self.ctx.compute.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        future = self.submit_task(
            label="postprocess",
            script=self.inputs.scripts.postprocess,
            arguments=["result.txt", "summary.txt"],
            retrieve_files=[
                "summary.txt",
                "timing_postprocess.txt",
            ],
            source_folder=self.ctx.compute.outputs.retrieved,
            mappings=[
                {
                    "folder": "previous",
                    "source": "result.txt",
                    "target": "result.txt",
                }
            ],
        )
        return ToContext(postprocess=future)

    def results(self):
        if not self.ctx.postprocess.is_finished_ok:
            return self.exit_codes.ERROR_TASK_FAILED

        self.out("raw_folder", self.ctx.generate.outputs.retrieved)
        self.out("prepared_folder", self.ctx.preprocess.outputs.retrieved)
        self.out("result_folder", self.ctx.compute.outputs.retrieved)
        self.out("summary_folder", self.ctx.postprocess.outputs.retrieved)
