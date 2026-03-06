process TRANSFER_DATA_AWS {
    tag "${meta.id}"
    label 'process_low'
    label 'transfer_data_aws'

    conda "conda-forge::rclone=1.70.1"
    container "dockerreg01.accounts.ad.wustl.edu/cgl/rclone@sha256:1d82d564b8da03893be1b8c6e16557d45e10b496080fd7569a89f312e55c2bfe"

    input:
    tuple val(meta), val(s3_files), path(local_files, stageAs: "local_files/*")

    output:
    path("transfer_data_aws.log"), emit: transfer_logs
    path("versions.yml")         , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix
    """
    export RCLONE_CONFIG_SOURCE_S3_TYPE=s3
    export RCLONE_CONFIG_SOURCE_S3_REGION=\$AWS_REGION
    export RCLONE_CONFIG_SOURCE_S3_ACCESS_KEY_ID=\$AWS_ACCESS_KEY
    export RCLONE_CONFIG_SOURCE_S3_SECRET_ACCESS_KEY=\$AWS_SECRET_KEY

    export RCLONE_CONFIG_DEST_S3_TYPE=s3
    export RCLONE_CONFIG_DEST_S3_REGION=\$GNX_REGION
    export RCLONE_CONFIG_DEST_S3_ACCESS_KEY_ID=\$GNX_ACCESS_KEY
    export RCLONE_CONFIG_DEST_S3_SECRET_ACCESS_KEY=\$GNX_SECRET_KEY

    BATCH_DEST_S3_FOLDER="dest_s3:\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/"
    if [ ${meta.id} != "Genoox_Metrics" ]; then
        BATCH_DEST_S3_FOLDER="\${BATCH_DEST_S3_FOLDER}${meta.id}/"
    fi

    # Create a single file list for all S3 files to transfer
    if [ ${s3_files.size()} -gt 0 ]; then
        echo "Preparing to transfer ${s3_files.size()} S3 files..."
        for full_path in ${s3_files.join(' ')}; do
            # Remove the remote name 'source_s3:' from the path for the --files-from list
            echo "\${full_path#source_s3:}" >> s3_files_to_transfer.txt
        done

        rclone copy \\
            source_s3: \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --files-from s3_files_to_transfer.txt \\
            --progress \\
            --retries 10 \\
            --no-traverse \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            || { echo "Rclone failed for S3 files."; cat transfer_data_aws.log; exit 1; }
    fi

    # Copy local files if present
    if [[ -d "local_files" ]] && [[ -n "${local_files}" ]]; then
        echo "Uploading local files from local_files/"

        rclone copy \\
            "local_files/" \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --progress \\
            --retries 10 \\
            --copy-links \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            || { echo "Rclone failed for local_files/*."; cat transfer_data_aws.log; exit 1; }
    fi

    echo "Finished sample: ${meta.id}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        rclone: \$(rclone --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix
    """
    export RCLONE_CONFIG_SOURCE_S3_TYPE=s3
    export RCLONE_CONFIG_SOURCE_S3_REGION=\$AWS_REGION
    export RCLONE_CONFIG_SOURCE_S3_ACCESS_KEY_ID=\$AWS_ACCESS_KEY
    export RCLONE_CONFIG_SOURCE_S3_SECRET_ACCESS_KEY=\$AWS_SECRET_KEY

    export RCLONE_CONFIG_DEST_S3_TYPE=s3
    export RCLONE_CONFIG_DEST_S3_REGION=\$GNX_REGION
    export RCLONE_CONFIG_DEST_S3_ACCESS_KEY_ID=\$GNX_ACCESS_KEY
    export RCLONE_CONFIG_DEST_S3_SECRET_ACCESS_KEY=\$GNX_SECRET_KEY

    BATCH_DEST_S3_FOLDER="dest_s3:\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/"
    if [ ${meta.id} != "Genoox_Metrics" ]; then
        BATCH_DEST_S3_FOLDER="\${BATCH_DEST_S3_FOLDER}${meta.id}/"
    fi

    # Create a single file list for all S3 files to transfer
    if [ ${s3_files.size()} -gt 0 ]; then
        echo "Preparing to transfer ${s3_files.size()} S3 files..."
        for full_path in ${s3_files.join(' ')}; do
            # Remove the remote name 'source_s3:' from the path for the --files-from list
            echo "\${full_path#source_s3:}" >> s3_files_to_transfer.txt
        done

        rclone copy \\
            source_s3: \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --files-from s3_files_to_transfer.txt \\
            --progress \\
            --retries 10 \\
            --no-traverse \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            --dry-run
    fi

    # Copy local files if present
    if [[ -d "local_files" ]] && [[ -n "${local_files}" ]]; then
        echo "Uploading local files from local_files/"

        rclone copy \\
            "local_files/" \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --progress \\
            --retries 10 \\
            --copy-links \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            --dry-run
    fi

    echo "Finished sample: ${meta.id}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        rclone: \$(rclone --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """
}
