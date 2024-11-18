process TRANSFER_DATA_AWS {
    label 'process_low'
    label 'gnx_aws'

    conda "conda-forge::awscli=2.15.39"
    container "docker.io/gregorysprenger/aws-cli:v2.15.39"

    input:
    path(dragen_align_files), stageAs: "dragen_align_files/*"
    path(joint_vcf_files)   , stageAs: "joint_called_files/*"
    path(joint_metric_files), stageAs: "joint_called_files/*"
    path(qc_metrics)

    output:
    path("versions.yml"), emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix
    """
    export AWS_ACCESS_KEY_ID=\${GNX_ACCESS_KEY}
    export AWS_SECRET_ACCESS_KEY=\${GNX_SECRET_KEY}

    # Gather all BAM files
    files=( \$(find -L dragen_align_files/ -type f -name "*.bam") )

    # Make directories and move files
    for file in \${files[@]}; do
        base=\$(basename \${file%%.*})
        mkdir -p "${prefix.id}/\${base}"

        for dir in dragen_align_files joint_called_files; do
            find -L \$dir \\
                -type f \\
                -name "\${base}*" \\
                -exec mv -f "{}" "${prefix.id}/\${base}" \\;
        done
    done

    # Move Genoox excel spreadsheet
    find -L \$PWD \\
        -type f \\
        -name "*Genoox.xlsx" \\
        -exec mv -f "{}" "${prefix.id}" \\;

    # Sync files with the following extensions
    aws s3 sync \\
        "${prefix.id}" \\
        "s3://\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/" \\
        --exclude "*" \\
        --include "*.bw*" \\
        --include "*.csv" \\
        --include "*.bam*" \\
        --include "*.json" \\
        --include "*.xlsx" \\
        --include "*.gff3*" \\
        --include "*.vcf.gz*" \\
        --include "*.bed.gz*" \\
        --include "*_usage.txt" \\
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

    # Gather all BAM files
    files=( \$(find -L dragen_align_files/ -type f -name "*.bam") )

    # Make directories and move files
    for file in \${files[@]}; do
        base=\$(basename \${file%%.*})
        mkdir -p "${prefix.id}/\${base}"

        for dir in dragen_align_files joint_called_files; do
            find -L \$dir \\
                -type f \\
                -name "\${base}*" \\
                -exec mv -f "{}" "${prefix.id}/\${base}" \\;
        done
    done

    # Move Genoox excel spreadsheet
    find -L \$PWD \\
        -type f \\
        -name "*Genoox.xlsx" \\
        -exec mv -f "{}" "${prefix.id}" \\;

    # Sync files with the following extensions
    aws s3 sync \\
        "${prefix.id}" \\
        "s3://\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/" \\
        --exclude "*" \\
        --include "*.bw*" \\
        --include "*.csv" \\
        --include "*.bam*" \\
        --include "*.json" \\
        --include "*.xlsx" \\
        --include "*.gff3*" \\
        --include "*.vcf.gz*" \\
        --include "*.bed.gz*" \\
        --include "*_usage.txt" \\
        --dryrun \\
        &> aws_log.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        aws: \$(aws --version 2>&1 | awk '{print \$1}' | cut -d '/' -f2)
    END_VERSIONS
    """
}
