nextflow.enable.dsl=2

process GENERATE_INPUT {
    output:
    path 'raw_input.txt'

    script:
    """
    python3 ${projectDir}/benchmark_scripts/generate_input.py raw_input.txt
    """
}

process PREPROCESS {
    input:
    path raw_file

    output:
    path 'prepared_input.txt'

    script:
    """
    python3 ${projectDir}/benchmark_scripts/preprocess.py ${raw_file} prepared_input.txt
    """
}

process COMPUTE {
    input:
    path prepared_file

    output:
    path 'result.txt'

    script:
    """
    python3 ${projectDir}/benchmark_scripts/compute_medium.py ${prepared_file} result.txt
    """
}

process POSTPROCESS {
    input:
    path result_file

    output:
    path 'summary.txt'

    script:
    """
    python3 ${projectDir}/benchmark_scripts/postprocess.py ${result_file} summary.txt
    """
}

workflow {
    raw_ch = GENERATE_INPUT()
    prepared_ch = PREPROCESS(raw_ch)
    result_ch = COMPUTE(prepared_ch)
    summary_ch = POSTPROCESS(result_ch)
}
