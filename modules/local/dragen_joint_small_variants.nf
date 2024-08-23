process DRAGEN_JOINT_SMALL_VARIANTS {
    tag "${small_variant_files[0].toString().split('\\.')[0]}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el8.4.3.6' :
        'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    path(small_variant_files)
    path(reference_directory)

    output:
    tuple val(task.ext.prefix), path("${task.ext.prefix.id}.vcf.gz"), emit: joint_small_variants
    tuple val(task.ext.prefix), path("*hard-filtered.vcf.gz")       , emit: joint_small_variants_filtered
    path("joint_small_variants_usage.txt")                          , emit: usage                        , optional: true
    path("*.vc_metrics.csv")                                        , emit: metrics
    path("versions.yml")                                            , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix              = task.ext.prefix
    def ref_dir             = reference_directory ? "--ref-dir ${reference_directory}" : ""
    def small_variants_list = small_variant_files.collect{ "--variant $it" }.join(' ')
    """
    /opt/dragen/4.3.6/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${small_variants_list} \\
        --output-directory \$PWD \\
        --enable-joint-genotyping true \\
        --output-file-prefix ${prefix.id}

    # Copy and rename DRAGEN usage
    find \$PWD \\
        -type f \\
        -name "*_usage.txt" \\
        -exec cp "{}" "joint_small_variants_usage.txt" \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/dragen/4.3.6/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version      = "4.3.6"
    def prefix              = task.ext.prefix
    def ref_dir             = reference_directory ? "--ref-dir ${reference_directory}" : ""
    def small_variants_list = small_variant_files.collect{ "--variant $it" }.join(' ')
    """
    cat <<-END_CMDS > "${prefix.id}.txt"
    /opt/dragen/4.3.6/bin/dragen \\
        --force \\
        ${ref_dir} \\
        ${small_variants_list} \\
        --output-directory \$PWD \\
        --enable-joint-genotyping true \\
        --output-file-prefix ${prefix.id}
    END_CMDS

    cp -f ${projectDir}/assets/test_data/dragen_path/joint_genotyped_vcf/*.vcf.gz \$PWD
    for file in *.vcf.gz; do
        mv \$file \${file/joint_genotyped/${prefix.id}}
    done

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
