from aiida.common.datastructures import CalcInfo, CodeInfo
from aiida.engine import CalcJob
from aiida.orm import Dict, FolderData, List, SinglefileData


class PythonTaskCalcJob(CalcJob):
    """Führt eines der vorhandenen Benchmark-Python-Skripte extern aus."""

    @classmethod
    def define(cls, spec):
        super().define(spec)

        spec.input("script", valid_type=SinglefileData)
        spec.input("timing_script", valid_type=SinglefileData)
        spec.input("arguments", valid_type=List)
        spec.input("retrieve_files", valid_type=List)

        spec.input_namespace(
            "source_folders",
            valid_type=FolderData,
            dynamic=True,
            required=False,
        )
        spec.input(
            "file_mappings",
            valid_type=Dict,
            required=False,
        )

    def prepare_for_submission(self, _folder):
        script_name = self.inputs.script.filename
        timing_script_name = self.inputs.timing_script.filename

        codeinfo = CodeInfo()
        codeinfo.code_uuid = self.inputs.code.uuid
        codeinfo.cmdline_params = [
            script_name,
            *self.inputs.arguments.get_list(),
        ]
        codeinfo.stdout_name = "aiida_stdout.txt"
        codeinfo.stderr_name = "aiida_stderr.txt"

        calcinfo = CalcInfo()
        calcinfo.codes_info = [codeinfo]

        # Das auszuführende Skript und seine normale Python-Abhängigkeit.
        calcinfo.local_copy_list = [
            (
                self.inputs.script.uuid,
                self.inputs.script.filename,
                script_name,
            ),
            (
                self.inputs.timing_script.uuid,
                self.inputs.timing_script.filename,
                timing_script_name,
            ),
        ]

        # Nur explizit angegebene fachliche Dateien aus vorherigen Jobs übernehmen.
        mappings = self.inputs.file_mappings.get_dict().get("files", []) \
            if "file_mappings" in self.inputs else []

        for mapping in mappings:
            folder_name = mapping["folder"]
            source_file = mapping["source"]
            target_file = mapping["target"]

            source_folder = self.inputs.source_folders[folder_name]

            calcinfo.local_copy_list.append(
                (
                    source_folder.uuid,
                    source_file,
                    target_file,
                )
            )

        retrieve_files = list(self.inputs.retrieve_files.get_list())

        for filename in ("aiida_stdout.txt", "aiida_stderr.txt"):
            if filename not in retrieve_files:
                retrieve_files.append(filename)

        calcinfo.retrieve_list = retrieve_files

        return calcinfo
