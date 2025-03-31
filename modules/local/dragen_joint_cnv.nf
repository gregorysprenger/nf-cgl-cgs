process DRAGEN_JOINT_CNV {
    tag "${cnv_files[0].toString().split('\\.')[0]}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el8.4.3.6' :
        'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    path(cnv_files)
    path(reference_directory)

    output:
    path("*cnv.vcf.gz")        , emit: joint_cnv
    path("joint_cnv_usage.txt"), emit: usage      , optional: true
    path("*.cnv_metrics.csv")  , emit: metrics
    path("versions.yml")       , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix   = task.ext.prefix
    def ref_dir  = reference_directory ? "--ref-dir ${reference_directory}" : ""
    def cnv_list = cnv_files.collect{ "--cnv-input $it" }.join(' ')
    """
    /opt/dragen/4.3.6/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${cnv_list} \\
        --enable-cnv true \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}

    # Copy and rename DRAGEN usage
    find \$PWD \\
        -type f \\
        -name "*_usage.txt" \\
        -exec cp "{}" "joint_cnv_usage.txt" \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/dragen/4.3.6/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version = "4.3.6"
    def prefix         = task.ext.prefix
    def ref_dir        = reference_directory ? "--ref-dir ${reference_directory}" : ""
    def cnv_list       = cnv_files.collect{ "--cnv-input $it" }.join(' ')
    """
    cat <<-END_CMDS > "${prefix.id}.txt"
    /opt/dragen/4.3.6/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${cnv_list} \\
        --enable-cnv true \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}
    END_CMDS

    cp -f ${projectDir}/assets/test_data/dragen_path/joint_genotyped_vcf/joint_genotyped.vcf.gz .
    mv joint_genotyped.vcf.gz ${prefix.id}.cnv.vcf.gz

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
