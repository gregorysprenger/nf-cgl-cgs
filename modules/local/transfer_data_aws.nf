process TRANSFER_DATA_AWS {
    label 'process_low'
    label 'gnx_aws'

    conda "conda-forge::awscli=2.15.39"
    container "docker.io/gregorysprenger/aws-cli:v2.15.39"

    input:
    tuple val(meta), path(dragen_align_files, stageAs: "dragen_align_files/*")
    path(joint_vcf_files, stageAs: "joint_called_files/*")
    path(joint_metric_files, stageAs: "joint_called_files/*")

    output:
    path("versions.yml"), emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix
    """
    export AWS_ACCESS_KEY_ID=\${GNX_ACCESS_KEY}
    export AWS_SECRET_ACCESS_KEY=\${GNX_SECRET_KEY}

    # Replace original dragen files with joint called files
    find joint_called_files \\
        -type f \\
        -name "${meta.id}*" \\
        -exec mv -f "{}" dragen_align_files/ \\;

    # Sync files with the following extensions
    aws s3 sync \\
        dragen_align_files/ \\
        "s3://\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/${meta.id}/" \\
        --exclude "*" \\
        --include "*.bw*" \\
        --include "*.csv" \\
        --include "*.bam*" \\
        --include "*.json" \\
        --include "*.gff3*" \\
        --include "*.vcf.gz*" \\
        --include "*.bed.gz*" \\
        &> aws_log.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        aws: \$(aws --version 2>&1 | awk '{print \$1}' | cut -d '/' -f2)
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix
    """
    export AWS_ACCESS_KEY_ID=\${GNX_ACCESS_KEY}
    export AWS_SECRET_ACCESS_KEY=\${GNX_SECRET_KEY}

    # Replace original dragen files with joint called files
    find joint_called_files \\
        -type f \\
        -name "${meta.id}*" \\
        -exec mv -f "{}" dragen_align_files/ \\;

    # Sync files with the following extensions
    aws s3 sync \\
        dragen_align_files/ \\
        "s3://\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/${meta.id}/" \\
        --exclude "*" \\
        --include "*.bw*" \\
        --include "*.csv" \\
        --include "*.bam*" \\
        --include "*.json" \\
        --include "*.gff3*" \\
        --include "*.vcf.gz*" \\
        --include "*.bed.gz*" \\
        --dryrun \\
        &> aws_log.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        aws: \$(aws --version 2>&1 | awk '{print \$1}' | cut -d '/' -f2)
    END_VERSIONS
    """
}
