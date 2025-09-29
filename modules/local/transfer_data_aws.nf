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
    export RCLONE_CONFIG_SOURCE_S3_REGION=\$AWS_REGION
    export RCLONE_CONFIG_SOURCE_S3_ACCESS_KEY_ID=\$AWS_ACCESS_KEY
    export RCLONE_CONFIG_SOURCE_S3_SECRET_ACCESS_KEY=\$AWS_SECRET_KEY

    export RCLONE_CONFIG_DEST_S3_REGION=\$GNX_REGION
    export RCLONE_CONFIG_DEST_S3_ACCESS_KEY_ID=\$GNX_ACCESS_KEY
    export RCLONE_CONFIG_DEST_S3_SECRET_ACCESS_KEY=\$GNX_SECRET_KEY

    BATCH_DEST_S3_FOLDER="dest_s3:\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/"
    if [ ${meta.id} != "Genoox_Metrics" ]; then
        BATCH_DEST_S3_FOLDER="\${BATCH_DEST_S3_FOLDER}${meta.id}/"
    fi

    mkdir -p lists

    # Group S3 files & write list files
    declare -A listfiles
    for full_path in ${s3_files.join(' ')}; do
        dir=\$(dirname "\$full_path")
        [[ "\$dir" != */ ]] && dir="\${dir}/"
        file=\$(basename "\$full_path")

        safe_key=\$(echo "\$dir" | sed 's/[^a-zA-Z0-9]/_/g')
        list_file="lists/\${safe_key}.txt"

        if [ -z "\${listfiles[\$dir]+x}" ]; then
            listfiles[\$dir]=\$list_file
            : > "\$list_file"
        fi

        printf '%s\n' "\$file" >> "\${listfiles[\$dir]}"
    done

    # Copy each directory’s files
    for dir in "\${!listfiles[@]}"; do
        list_file="\${listfiles[\$dir]}"
        nfiles=\$(wc -l < "\$list_file" | tr -d '[:space:]')
        echo "Directory: \$dir (\$nfiles files)"

        rclone copy \\
            "\$dir" \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --files-from "\$list_file" \\
            --progress \\
            --retries 10 \\
            --no-traverse \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            || error "Rclone failed for \$dir"
    done

    # Copy local files if present
    if [ -d "${local_files}" ] || [ -f "${local_files}" ]; then
        echo "Uploading local files from ${local_files}"

        rclone copy \\
            "${local_files}/" \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --progress \\
            --retries 10 \\
            --copy-links \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            || error "Rclone failed for local_files/*"
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
    export RCLONE_CONFIG_SOURCE_S3_REGION=\$AWS_REGION
    export RCLONE_CONFIG_SOURCE_S3_ACCESS_KEY_ID=\$AWS_ACCESS_KEY
    export RCLONE_CONFIG_SOURCE_S3_SECRET_ACCESS_KEY=\$AWS_SECRET_KEY

    export RCLONE_CONFIG_DEST_S3_REGION=\$GNX_REGION
    export RCLONE_CONFIG_DEST_S3_ACCESS_KEY_ID=\$GNX_ACCESS_KEY
    export RCLONE_CONFIG_DEST_S3_SECRET_ACCESS_KEY=\$GNX_SECRET_KEY

    BATCH_DEST_S3_FOLDER="dest_s3:\${GNX_BUCKET}/\${GNX_DATA}/${prefix.id}/"
    if [ ${meta.id} != "Genoox_Metrics" ]; then
        BATCH_DEST_S3_FOLDER="\${BATCH_DEST_S3_FOLDER}${meta.id}/"
    fi

    mkdir -p lists

    # Group S3 files & write list files
    declare -A listfiles
    for full_path in ${s3_files.join(' ')}; do
        dir=\$(dirname "\$full_path")
        [[ "\$dir" != */ ]] && dir="\${dir}/"
        file=\$(basename "\$full_path")

        safe_key=\$(echo "\$dir" | sed 's/[^a-zA-Z0-9]/_/g')
        list_file="lists/\${safe_key}.txt"

        if [ -z "\${listfiles[\$dir]+x}" ]; then
            listfiles[\$dir]=\$list_file
            : > "\$list_file"
        fi

        printf '%s\n' "\$file" >> "\${listfiles[\$dir]}"
    done

    # Copy each directory’s files
    for dir in "\${!listfiles[@]}"; do
        list_file="\${listfiles[\$dir]}"
        nfiles=\$(wc -l < "\$list_file" | tr -d '[:space:]')
        echo "Directory: \$dir (\$nfiles files)"

        rclone copy \\
            "\$dir" \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --files-from "\$list_file" \\
            --progress \\
            --retries 10 \\
            --no-traverse \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            --dry-run \\
            || error "Rclone failed for \$dir"
    done

    # Copy local files if present
    if [ -d "${local_files}" ] || [ -f "${local_files}" ]; then
        echo "Uploading local files from ${local_files}"

        rclone copy \\
            "${local_files}/" \\
            "\$BATCH_DEST_S3_FOLDER" \\
            --progress \\
            --retries 10 \\
            --copy-links \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file transfer_data_aws.log \\
            --s3-location-constraint "\${RCLONE_CONFIG_DEST_S3_REGION}" \\
            --dry-run \\
            || error "Rclone failed for local_files/*"
    fi

    echo "Finished sample: ${meta.id}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        rclone: \$(rclone --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """
}
