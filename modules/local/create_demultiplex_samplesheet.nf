process CREATE_DEMULTIPLEX_SAMPLESHEET {
    tag "${illumina_run_dir.name}"
    label 'process_low'

    container "ghcr.io/dhslab/docker-python3:231224"

    input:
    tuple val(flowcell), path(samplesheet), path(illumina_run_dir)

    output:
    tuple val(flowcell), path("*demux_samplesheet.csv"), emit: samplesheet
    path("*runinfo.csv")                               , emit: runinfo
    path("versions.yml")                               , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    prepare_dragen_demux.py \\
        -r ${illumina_run_dir} \\
        -s ${samplesheet}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """

    stub:
    """
    touch ${flowcell}.runinfo.csv \\
        ${flowcell}.demux_samplesheet.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """
}
