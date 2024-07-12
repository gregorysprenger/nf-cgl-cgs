process CREATE_DEMULTIPLEX_SAMPLESHEET {
    label 'process_low'
    container "ghcr.io/dhslab/docker-python3:231224"

    input:
    path(samplesheet)
    path(illumina_run_dir)

    output:
    path("*demux_samplesheet.csv"), emit: samplesheet
    path("*runinfo.csv")          , emit: runinfo
    path("versions.yml")          , emit: versions

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
    touch STUB.demux_samplesheet.csv STUB.runinfo.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """
}
