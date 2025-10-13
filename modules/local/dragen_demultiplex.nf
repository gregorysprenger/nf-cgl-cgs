process DRAGEN_DEMULTIPLEX {
    tag "${task.ext.prefix.id}"
    label 'dragen'

    container "${ ['awsbatch'].any{ workflow.profile.contains(it) }
        ? 'job-definition://dragen_v4-3-6'
        : 'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    tuple val(meta), path(samplesheet), path(illumina_run_dir)

    output:
    path("${task.ext.prefix.id}/Reports/fastq_list.csv"), emit: fastq_list
    path("${task.ext.prefix.id}/*")                     , emit: demux_files
    path("versions.yml")                                , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix   = task.ext.prefix
    def exe_path = ['dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    """
    # Perform demultiplexing of samples
    ${exe_path}/bin/dragen \\
        --bcl-conversion-only true \\
        --bcl-only-matched-reads true \\
        --strict-mode true \\
        --sample-sheet ${samplesheet} \\
        --bcl-input-directory ${illumina_run_dir} \\
        --output-directory "\$PWD/${prefix.id}"

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
    def exe_path = ['dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    """
    cp -r ${projectDir}/assets/20250606_A22VNC2LT3/demux_fastq "${prefix.id}"

    cat <<-END_CMDS > "${prefix.id}_cmds.txt"
    ${exe_path}/bin/dragen \\
        --bcl-conversion-only true \\
        --bcl-only-matched-reads true \\
        --strict-mode true \\
        --sample-sheet ${samplesheet} \\
        --bcl-input-directory ${illumina_run_dir} \\
        --output-directory "\$PWD/${prefix.id}"
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """
}
