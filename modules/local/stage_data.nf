process STAGE_DATA {
    label 'process_low'
    label 'stage_data'

    conda "conda-forge::rclone=1.70.1"
    container "dockerreg01.accounts.ad.wustl.edu/cgl/rclone@sha256:1d82d564b8da03893be1b8c6e16557d45e10b496080fd7569a89f312e55c2bfe"

    input:
    tuple val(meta), val(s3_files), path(local_files, stageAs: "local_files/*")

    output:
    tuple val(meta), path("${meta.id}/*"), emit: staged_files
    path("stage_data.log")               , emit: stage_log   , optional: true
    path("versions.yml")                 , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def files = [s3_files].flatten().findAll().join('\n')
    """
    export RCLONE_CONFIG_SOURCE_S3_TYPE=s3
    export RCLONE_CONFIG_SOURCE_S3_PROVIDER=AWS
    export RCLONE_CONFIG_SOURCE_S3_REGION=\$AWS_REGION
    export RCLONE_CONFIG_SOURCE_S3_ACCESS_KEY_ID=\$AWS_ACCESS_KEY
    export RCLONE_CONFIG_SOURCE_S3_SECRET_ACCESS_KEY=\$AWS_SECRET_KEY

    mkdir -p ${meta.id}
    printf "%s" "${files}" > file_list.txt

    if [[ -s file_list.txt ]]; then
        mkdir -p staging

        echo "Downloading files from file_list.txt to staging/"
        rclone copy \\
            --files-from file_list.txt \\
            source_s3: \\
            staging/ \\
            --progress \\
            --retries 10 \\
            --copy-links \\
            --log-level INFO \\
            --transfers ${task.cpus} \\
            --log-file stage_data.log \\
            || { echo "Error: Rclone download failed for files in file_list.txt"; exit 1; }

        echo "Moving files from staging/ to ${meta.id}/"
        find staging/ -type f -exec mv {} "${meta.id}/" \\;
    else
        echo "No S3 files to download."
    fi

    if [[ -d "local_files" ]] && [[ -n "${local_files}" ]]; then
        echo "Staging local files from local_files/"
        cp -RL local_files/* "${meta.id}/"
    else
        echo "No local files to stage."
    fi

    echo "Finished sample: ${meta.id}"

    echo "${task.process}:" > versions.yml
    echo "  rclone: \$(rclone --version | head -n 1 | cut -d ' ' -f2)" >> versions.yml
    """

    stub:
    def files = [s3_files].flatten().findAll().join('\n')
    """
    export RCLONE_CONFIG_SOURCE_S3_TYPE=s3
    export RCLONE_CONFIG_SOURCE_S3_PROVIDER=AWS
    export RCLONE_CONFIG_SOURCE_S3_REGION=\$AWS_REGION
    export RCLONE_CONFIG_SOURCE_S3_ACCESS_KEY_ID=\$AWS_ACCESS_KEY
    export RCLONE_CONFIG_SOURCE_S3_SECRET_ACCESS_KEY=\$AWS_SECRET_KEY

    mkdir -p ${meta.id}
    printf "%s" "${files}" > file_list.txt

    if [[ -s file_list.txt ]]; then
        while IFS= read -r line; do
            filename=\$(basename "\$line")
            touch "${meta.id}/\$filename"
            echo "Stub download: \$line -> ${meta.id}/\$filename"
        done < file_list.txt
    else
        echo "No S3 files to stub."
    fi

    if [[ -d "local_files" ]] && [[ -n "${local_files}" ]]; then
        for f in ${local_files}; do
            filename=\$(basename "\$f")
            touch "${meta.id}/\$filename"
            echo "Stub local: \$f -> ${meta.id}/\$filename"
        done
    else
        echo "No local files to stage."
    fi

    echo "Finished sample: ${meta.id}"

    echo "${task.process}:" > versions.yml
    echo "  rclone: \$(rclone --version | head -n 1 | cut -d ' ' -f2)" >> versions.yml
    """
}
