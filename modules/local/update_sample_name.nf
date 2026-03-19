process UPDATE_SAMPLE_NAME {
    tag "${meta.id}"
    label 'process_low'

    container 'mgibio/samtools:v1.21-noble'

    input:
    tuple val(meta), path(alignment_file)

    output:
    tuple val(meta), path("*.updated.*"), emit: updated_alignment
    path("versions.yml")                , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    base_name=\$(basename "${alignment_file}")
    file_extension="\${base_name##*.}"
    new_filename="\${base_name%.*}.updated.\${file_extension}"

    samtools view -H ${alignment_file} \\
        | sed "s/SM:[^\t]*/SM:${meta.id}/g" \\
        > header.sam

    if [[ "\${file_extension,,}" == "cram" ]]; then
        cp "${alignment_file}" "\${new_filename}"
        samtools reheader --in-place header.sam "\${new_filename}"
    else
        samtools reheader header.sam "${alignment_file}" > "\${new_filename}"
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        samtools: \$(samtools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """

    stub:
    """
    base_name=\$(basename "${alignment_file}")
    file_extension="\${base_name##*.}"
    new_filename="\${base_name%.*}.updated.\${file_extension}"

    touch "\${new_filename}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        samtools: \$(samtools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """
}
