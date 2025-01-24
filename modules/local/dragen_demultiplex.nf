process DRAGEN_DEMULTIPLEX {
    tag "${task.ext.prefix.id}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el8.4.3.6' :
        'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    path(samplesheet)
    path(rundir)

    output:
    path("${task.ext.prefix.id}/Reports/fastq_list.csv"), emit: fastq_list
    path("${task.ext.prefix.id}/*")                     , emit: demux_files
    path("versions.yml")                                , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix
    """
    # Perform demultiplexing of samples
    /opt/dragen/4.3.6/bin/dragen \\
        --bcl-conversion-only true \\
        --bcl-only-matched-reads true \\
        --strict-mode true \\
        --sample-sheet ${samplesheet} \\
        --bcl-input-directory ${rundir} \\
        --output-directory "\$PWD/${prefix.id}"

    # Copy RunParameters.xml to ${prefix.id}/Reports
    find -L ${rundir} \\
        -type f \\
        -name "RunParameters.xml" \\
        -exec cp "{}" ${prefix.id}/Reports/ \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/dragen/4.3.6/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version = "4.3.6"
    def prefix         = task.ext.prefix
    """
    cp -r ${projectDir}/assets/test_data/demux_fastq/ ${prefix.id}

    cat <<-END_CMDS > "${prefix.id}_cmds.txt"
        /opt/dragen/4.3.6/bin/dragen \\
            --bcl-conversion-only true \\
            --bcl-only-matched-reads true \\
            --strict-mode true \\
            --sample-sheet ${samplesheet} \\
            --bcl-input-directory ${rundir} \\
            --output-directory "\$PWD/${prefix.id}"
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
