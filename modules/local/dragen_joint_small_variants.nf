process DRAGEN_JOINT_SMALL_VARIANTS {
    tag "${task.ext.prefix.id}"
    label 'dragen'

    container "${ ['dragenaws'].any{ workflow.profile.contains(it) }
        ? 'job-definition://dragen-v4-3-6:1'
        : 'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    path(small_variant_files)
    path(reference_directory)

    output:
    path("${task.ext.prefix.id}.vcf.gz")  , emit: joint_small_variants
    path("*hard-filtered.vcf.gz")         , emit: joint_small_variants_filtered
    path("joint_small_variants_usage.txt"), emit: usage                        , optional: true
    path("*.vc_metrics.csv")              , emit: metrics
    path("versions.yml")                  , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix      = task.ext.prefix
    def exe_path    = ['dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    def dragen_args = [
        reference_directory ? "--ref-dir ${reference_directory}" : "",
        small_variant_files.collect{ "--variant $it" }.join(' ')
    ].join(' ').trim()
    """
    ${exe_path}/bin/dragen \\
        --force \\
        ${dragen_args} \\
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
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def prefix      = task.ext.prefix
    def exe_path    = ['dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    def dragen_args = [
        reference_directory ? "--ref-dir ${reference_directory}" : "",
        small_variant_files.collect{ "--variant $it" }.join(' ')
    ].join(' ').trim()
    """
    for file in "${projectDir}/assets/test_data/dragen_path/joint_genotyped_vcf/"*; do
        cp "\$file" "\$(basename "\$file" | sed "s/joint_genotyped/${prefix.id}/")"
    done

    cat <<-END_CMDS > "${prefix.id}.txt"
    ${exe_path}/bin/dragen \\
        --force \\
        ${dragen_args} \\
        --output-directory \$PWD \\
        --enable-joint-genotyping true \\
        --output-file-prefix ${prefix.id}
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """
}
