process DRAGEN_JOINT_SV {
    tag "${meta.id}"
    label 'dragen'
    label 'dragenalign'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el7.4.2.4' :
        'docker.io/etycksen/dragen4:4.2.4' }"

    input:
    tuple val(meta), path(sv_files)

    output:

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix  = task.ext.prefix
    def ref_dir = params.dragen_ref_dir ? "--ref-dir ${params.dragen_ref_dir}" : ""
    def sv_list = sv_files.collect{ "--bam-input $it" }.join(' \\\\\n')
    """
    /opt/edico/bin/dragen \\
        --force \\
        ${sv_list} \\
        ${ref_dir} \\
        --enable-sv true \\
        --enable-map-align false \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version   = "4.2.4"
    def ref_dir = params.dragen_ref_dir ? "--ref-dir ${params.dragen_ref_dir}" : ""
    def sv_list = sv_files.collect{ "--bam-input $it" }.join(' \\\\\n')
    """
    cat <<-END_CMDS > "${meta.id}.txt"
    /opt/edico/bin/dragen \\
        --force \\
        ${sv_list} \\
        ${ref_dir} \\
        --enable-sv true \\
        --enable-map-align false \\
        --output-directory \$PWD \\
        --output-file-prefix ${meta.id}
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
