process DRAGEN_JOINT_CNV {
    tag "${meta.id}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el7.4.2.4' :
        'docker.io/etycksen/dragen4:4.2.4' }"

    input:
    path(cnv_files)

    output:
    tuple val(task.ext.prefix), path("*cnv.vcf.gz"), emit: joint_cnv
    path("versions.yml")                           , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix   = task.ext.prefix
    def ref_dir  = params.dragen_ref_dir ? "--ref-dir ${params.dragen_ref_dir}" : ""
    def cnv_list = cnv_files.collect{ "--cnv-input $it" }.join(' \\\\\n')
    """
    /opt/edico/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${cnv_list} \\
        --enable-cnv true \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version = "4.2.4"
    def prefix         = task.ext.prefix
    def ref_dir        = params.dragen_ref_dir ? "--ref-dir ${params.dragen_ref_dir}" : ""
    def cnv_list       = cnv_files.collect{ "--cnv-input $it" }.join(' ')
    """
    cat <<-END_CMDS > "${prefix.id}.txt"
    /opt/edico/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${cnv_list} \\
        --enable-cnv true \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
