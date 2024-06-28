process DRAGEN_JOINT_CNV {
    tag "${meta.id}"
    label 'dragen'
    label 'dragenalign'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el7.4.2.4' :
        'etycksen/dragen4:4.2.4' }"

    input:
    tuple val(meta), path(cnv_files)

    output:
    tuple val(meta), path("*cnv.vcf.gz"), emit: joint_cnv
    path("versions.yml")                , emit: versions

    script:
    def ref_dir  = params.dragen_ref_dir ? "--ref-dir ${params.dragen_ref_dir}" : ""
    def cnv_list = cnv_files.collect{ "--cnv-input $it" }.join(' \\\\\n')
    """
    /opt/edico/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${cnv_list} \\
        --enable-cnv true \\
        --output-directory \$PWD \\
        --output-file-prefix ${meta.id}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version   = "4.2.4"
    def ref_dir  = params.dragen_ref_dir ? "--ref-dir ${params.dragen_ref_dir}" : ""
    def cnv_list = cnv_files.collect{ "--cnv-input $it" }.join(' ')
    """
    cat <<-END_CMDS > "${meta.id}.txt"
    /opt/edico/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${cnv_list} \\
        --enable-cnv true \\
        --output-directory \$PWD \\
        --output-file-prefix ${meta.id}
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
