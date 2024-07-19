process DRAGEN_JOINT_SV {
    tag "${sv_files[0].toString().split('\\.')[0]}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el7.4.2.4' :
        'docker.io/etycksen/dragen4:4.2.4' }"

    input:
    path(sv_files)

    output:
    tuple val(task.ext.prefix), path("*.vcf.gz"), emit: joint_sv
    path("versions.yml")                        , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix  = task.ext.prefix
    def ref_dir = params.refdir ? "--ref-dir ${params.refdir}" : ""
    def sv_list = sv_files.collect{ "--bam-input $it" }.join(' \\\\n')
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
    def dragen_version = "4.2.4"
    def prefix         = task.ext.prefix
    def ref_dir        = params.refdir ? "--ref-dir ${params.refdir}" : ""
    def sv_list        = sv_files.collect{ "--bam-input $it" }.join(' \\\\n')
    """
    cat <<-END_CMDS > "${prefix.id}.txt"
    /opt/edico/bin/dragen \\
        --force \\
        ${sv_list} \\
        ${ref_dir} \\
        --enable-sv true \\
        --enable-map-align false \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}
    END_CMDS

    cp -f ${projectDir}/assets/test_data/dragen_path/joint_genotyped_vcf/joint_genotyped.vcf.gz .
    mv joint_genotyped.vcf.gz ${prefix.id}.sv.vcf.gz

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
