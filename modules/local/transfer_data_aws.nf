process TRANSFER_DATA_AWS {
    tag "${task.ext.prefix.id}"
    label 'process_low'
    label 'gnx_aws'

    conda "conda-forge::rclone=1.70.1"
    container "docker.io/gregorysprenger/rclone@sha256:3ad0d26b504902bfe8016932a31491fb354f2ef47072c100edfd74cff6537577"

    input:
    path(dragen_align_files), stageAs: "dragen_align_files/*"
    path(genoox_metrics)

    output:
    path("*log.txt")    , emit: transfer_logs
    path("versions.yml"), emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix
    """
    export AWS_ACCESS_KEY_ID='\$GNX_ACCESS_KEY'
    export AWS_SECRET_ACCESS_KEY='\$GNX_SECRET_KEY'
    export AWS_REGION='\$GNX_REGION'

    export RCLONE_CONFIG_S3_TYPE=s3
    export RCLONE_CONFIG_S3_PROVIDER=AWS
    export RCLONE_CONFIG_S3_ENV_AUTH=true

    # Structure data for S3 bucket
    find -L dragen_align_files/ -type f \\
        | while read -r file; do
            base=\$(basename \${file%%.*})
            mkdir -p "${prefix.id}/\${base}"
            mv -f "\${file}" "${prefix.id}/\${base}/"
        done

    mv -f ${genoox_metrics} "${prefix.id}/"

    # Copy files to S3 bucket
    rclone copy \\
        "${prefix.id}" \\
        "s3://\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/" \\
        --copy-links \\
        --log-file=rclone_log.txt \\
        --log-level INFO \\
        --progress \\
        --s3-chunk-size=256M \\
        --s3-region="\${AWS_REGION}" \\
        --s3-upload-concurrency=8 \\
        --transfers=16

    if [ \$? -eq 0 ]; then
        echo "AWS sync completed successfully." > aws_log.txt
    else
        error "AWS sync failed. Check 'rclone_log.txt' for details." > aws_log.txt
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        aws: \$(aws --version 2>&1 | awk '{print \$1}' | cut -d '/' -f2)
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix
    """
    export AWS_ACCESS_KEY_ID='\$GNX_ACCESS_KEY'
    export AWS_SECRET_ACCESS_KEY='\$GNX_SECRET_KEY'
    export AWS_REGION='\$GNX_REGION'

    export RCLONE_CONFIG_S3_TYPE=s3
    export RCLONE_CONFIG_S3_PROVIDER=AWS
    export RCLONE_CONFIG_S3_ENV_AUTH=true

    # Structure data for S3 bucket
    find -L dragen_align_files/ -type f \\
        | while read -r file; do
            base=\$(basename \${file%%.*})
            mkdir -p "${prefix.id}/\${base}"
            mv -f "\${file}" "${prefix.id}/\${base}/"
        done

    mv -f ${genoox_metrics} "${prefix.id}/"

    # Copy files to S3 bucket
    rclone copy \\
        "${prefix.id}" \\
        "s3://\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/" \\
        --copy-links \\
        --log-file=rclone_log.txt \\
        --log-level INFO \\
        --progress \\
        --s3-chunk-size=256M \\
        --s3-region="\${AWS_REGION}" \\
        --s3-upload-concurrency=8 \\
        --transfers=16 \\
        --dry-run

    if [ \$? -eq 0 ]; then
        echo "AWS sync completed successfully." > aws_log.txt
    else
        error "AWS sync failed. Check 'rclone_log.txt' for details." > aws_log.txt
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        aws: \$(aws --version 2>&1 | awk '{print \$1}' | cut -d '/' -f2)
    END_VERSIONS
    """
}
