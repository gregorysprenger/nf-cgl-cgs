process CREATE_DEMULTIPLEX_SAMPLESHEET {
    label 'process_low'
    container "ghcr.io/dhslab/docker-python3:231224"

    input:
    path mastersheet
    path rundir

    output:
    path("*.demux_samplesheet.csv"), emit: samplesheet
    path("*.runinfo.csv"), emit: runinfo
    path "versions.yml", emit: versions

    script:
    """
    prepare_dragen_demux.py -r ${rundir} -s ${mastersheet}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(prepare_dragen_demux.py --version)
    END_VERSIONS
    """

    stub:
    """
    touch STUB.demux_samplesheet.csv
    touch STUB.runinfo.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(prepare_dragen_demux.py --version)
    END_VERSIONS
    """
}
