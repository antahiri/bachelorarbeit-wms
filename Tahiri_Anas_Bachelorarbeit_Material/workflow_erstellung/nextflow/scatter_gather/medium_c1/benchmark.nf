nextflow.enable.dsl=2

params.chunks = 1

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

process SPLIT {
    input:
    path prepared_file

    output:
    path 'chunk_*.txt'

    script:
    """
    python3 ${projectDir}/benchmark_scripts/split.py ${prepared_file} ${params.chunks}
    """
}

process COMPUTE {
    input:
    path chunk_file

    output:
    path "result_${chunk_file.simpleName.replace('chunk_', '')}.txt"

    script:
    """
    chunk_id=\$(echo ${chunk_file.simpleName} | sed 's/chunk_//')
    python3 ${projectDir}/benchmark_scripts/compute_medium.py ${chunk_file} result_\${chunk_id}.txt
    """
}

process AGGREGATE {
    input:
    path result_files

    output:
    path 'aggregated_result.txt'

    script:
    """
    python3 ${projectDir}/benchmark_scripts/aggregate.py ${result_files} aggregated_result.txt
    """
}

process POSTPROCESS {
    input:
    path aggregated_file

    output:
    path 'summary.txt'

    script:
    """
    python3 ${projectDir}/benchmark_scripts/postprocess.py ${aggregated_file} summary.txt
    """
}

workflow {
    raw_ch = GENERATE_INPUT()

    prepared_ch = PREPROCESS(raw_ch)

    chunks_ch = SPLIT(prepared_ch)
        .flatten()

    results_ch = COMPUTE(chunks_ch)
        .collect()

    aggregated_ch = AGGREGATE(results_ch)

    summary_ch = POSTPROCESS(aggregated_ch)
}
