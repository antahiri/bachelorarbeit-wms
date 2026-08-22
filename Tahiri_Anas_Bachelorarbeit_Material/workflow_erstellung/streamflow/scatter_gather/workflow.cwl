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
