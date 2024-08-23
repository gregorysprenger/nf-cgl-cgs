process DRAGEN_JOINT_SV {
    tag "${sv_files[0].toString().split('\\.')[0]}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el8.4.3.6' :
        'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    path(sv_files)
    path(reference_directory)

    output:
    tuple val(task.ext.prefix), path("*.vcf.gz"), emit: joint_sv
    path("joint_sv_usage.txt")                  , emit: usage     , optional: true
    path("*.sv_metrics.csv")                    , emit: metrics
    path("versions.yml")                        , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix  = task.ext.prefix
    def ref_dir = reference_directory ? "--ref-dir ${reference_directory}" : ""
    def sv_list = sv_files.collect{ "--bam-input $it" }.join(' ')
    """
    /opt/dragen/4.3.6/bin/dragen \\
        --force \\
        ${sv_list} \\
        ${ref_dir} \\
        --enable-sv true \\
        --enable-map-align false \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}

    # Copy and rename DRAGEN usage
    find \$PWD \\
        -type f \\
        -name "*_usage.txt" \\
        -exec cp "{}" "joint_sv_usage.txt" \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/dragen/4.3.6/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version = "4.3.6"
    def prefix         = task.ext.prefix
    def ref_dir        = reference_directory ? "--ref-dir ${reference_directory}" : ""
    def sv_list        = sv_files.collect{ "--bam-input $it" }.join(' ')
    """
    cat <<-END_CMDS > "${prefix.id}.txt"
    /opt/dragen/4.3.6/bin/dragen \\
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
