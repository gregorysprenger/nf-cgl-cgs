process DRAGEN_DEMULTIPLEX {
    tag "${prefix.id}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el7.4.2.4' :
        'docker.io/etycksen/dragen4:4.2.4' }"

    input:
    path(samplesheet)
    path(rundir)

    output:

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix     = task.ext.prefix
    def first_tile = params.bcl_first_tile ? "--first-tile-only true" : ""
    """
    mkdir -p demux_fastq

    # Perform demultiplexing of samples
    /opt/edico/bin/dragen \\
        --bcl-conversion-only true \\
        --bcl-only-matched-reads true \\
        --strict-mode true \\
        ${first_tile} \\
        --sample-sheet ${samplesheet} \\
        --bcl-input-directory ${rundir} \\
        --output-directory demux_fastq

    # Copy RunParameters.xml to demux_fastq/Reports
    find ${run_dir} \\
        -type f \\
        -name "RunParameters.xml" \\
        -exec cp '{}' demux_fastq/Reports/ \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    """
    mkdir -p demux_fastq

    cp ${projectDir}/assets/stub/demux_fastq/Reports/fastq_list.csv demux_fastq/

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(cat ${projectDir}/assets/stub/versions/dragen_version.txt)
    END_VERSIONS
    """
}
