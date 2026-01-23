process DRAGEN_DEMULTIPLEX {
    tag "${task.ext.prefix.id}"
    label 'dragen'

    container "${ ['awsbatch'].any{ workflow.profile.contains(it) }
        ? 'job-definition://dragen_v4-3-6'
        : 'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    tuple val(meta), path(samplesheet), path(illumina_run_dir)

    output:
    path("fastq_list.scratch.csv") , emit: fastq_list
    path("${task.ext.prefix.id}/*"), emit: demux_files
    path("versions.yml")           , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix   = task.ext.prefix
    def exe_path = ['awsbatch'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    """
    # Perform demultiplexing of samples
    ${exe_path}/bin/dragen \\
        --bcl-conversion-only true \\
        --bcl-only-matched-reads true \\
        --strict-mode true \\
        --sample-sheet ${samplesheet} \\
        --bcl-input-directory ${illumina_run_dir} \\
        --output-directory "${prefix.id}"

    # Update fastq_list.csv with new paths
    sed \\
        "s|${prefix.id}/|\${PWD}/${prefix.id}/|g" \\
        "${prefix.id}/Reports/fastq_list.csv" \\
        > fastq_list.scratch.csv

    if [[ -n "${params.demux_outdir}" ]]; then
        sed \\
            -i "s|${prefix.id}/|${params.demux_outdir}/${prefix.id}/|g" \\
            "${prefix.id}/Reports/fastq_list.csv"
    fi

    # Copy RunParameters.xml to ${prefix.id}/Reports
    find -L ${illumina_run_dir} \\
        -type f \\
        -name "RunParameters.xml" \\
        -exec cp "{}" ${prefix.id}/Reports/ \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def prefix   = task.ext.prefix
    def exe_path = ['awsbatch'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    """
    cp -r ${projectDir}/assets/stub/demux_fastq "${prefix.id}"
    cp "${prefix.id}/Reports/fastq_list.csv" fastq_list.scratch.csv

    cat <<-END_CMDS > "${prefix.id}_cmds.txt"
    ${exe_path}/bin/dragen \\
        --bcl-conversion-only true \\
        --bcl-only-matched-reads true \\
        --strict-mode true \\
        --sample-sheet ${samplesheet} \\
        --bcl-input-directory ${illumina_run_dir} \\
        --output-directory "${prefix.id}"
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """
}
