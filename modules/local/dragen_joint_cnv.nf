process DRAGEN_JOINT_CNV {
    tag "${task.ext.prefix.id}"
    label 'dragen'

    container "${ ['awsbatch','dragenaws'].any{ workflow.profile.contains(it) }
        ? 'job-definition://dragen_v4-3-6'
        : 'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    path(cnv_files)
    path(reference_directory)

    output:
    path("*cnv.vcf.gz")        , emit: joint_cnv
    path("joint_cnv_usage.txt"), emit: usage    , optional: true
    path("*.cnv_metrics.csv")  , emit: metrics
    path("versions.yml")       , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix      = task.ext.prefix
    def exe_path    = ['awsbatch','dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    def dragen_args = [
        task.ext.dragen_license_args ?: "",
        reference_directory          ? "--ref-dir ${reference_directory}" : "",
        cnv_files.collect{ "--cnv-input $it" }.join(' '),
    ].join(' ').trim()
    """
    ${exe_path}/bin/dragen \\
        --force \\
        ${dragen_args} \\
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
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def prefix      = task.ext.prefix
    def exe_path    = ['awsbatch','dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"
    def dragen_args = [
        task.ext.dragen_license_args ?: "",
        reference_directory          ? "--ref-dir ${reference_directory}" : "",
        cnv_files.collect{ "--cnv-input $it" }.join(' '),
    ].join(' ').trim()
    """
    cp -f \\
        ${projectDir}/assets/test_data/dragen_path/joint_genotyped_vcf/joint_genotyped.vcf.gz \\
        ${prefix.id}.cnv.vcf.gz

    cat <<-END_CMDS > "${prefix.id}.txt"
    ${exe_path}/bin/dragen \\
        --force \\
        ${dragen_args} \\
        --enable-cnv true \\
        --output-directory \$PWD \\
        --output-file-prefix ${prefix.id}
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """
}
