process DRAGEN_DEMULTIPLEX {
    tag "${prefix.id}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el8.4.3.6' :
        'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    path(samplesheet)
    path(rundir)

    output:
    path("${task.ext.prefix}/fastq_list.csv"), emit: fastq_list
    path("${task.ext.prefix}/*")             , emit: demux_files
    path("*_usage.txt")                      , emit: usage      , optional: true
    path("versions.yml")                     , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix     = task.ext.prefix
    def first_tile = params.bcl_first_tile ? "--first-tile-only true" : ""
    """
    mkdir -p ${prefix.id}

    # Perform demultiplexing of samples
    /opt/dragen/4.3.6/bin/dragen \\
        --bcl-conversion-only true \\
        --bcl-only-matched-reads true \\
        --strict-mode true \\
        ${first_tile} \\
        --sample-sheet ${samplesheet} \\
        --bcl-input-directory ${rundir} \\
        --output-directory ${prefix.id}

    # Copy RunParameters.xml to ${prefix.id}/Reports
    find ${rundir} \\
        -type f \\
        -name "RunParameters.xml" \\
        -exec cp "{}" ${prefix.id}/Reports/ \\;

    # Copy and rename DRAGEN usage
    find \$PWD \\
        -type f \\
        -name "*_usage.txt" \\
        -exec cp "{}" "demultiplex_usage.txt" \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/dragen/4.3.6/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def prefix     = task.ext.prefix
    def first_tile = params.bcl_first_tile ? "--first-tile-only true" : ""
    """
    mkdir -p ${prefix.id}

    cp ${projectDir}/assets/stub/demux_fastq/Reports/fastq_list.csv ${prefix.id}/

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(cat ${projectDir}/assets/stub/versions/dragen_version.txt)
    END_VERSIONS
    """
}
