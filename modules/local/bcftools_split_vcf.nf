process BCFTOOLS_SPLIT_VCF {
    tag "${meta.id}"
    label 'process_medium'

    container "docker.io/mgibio/bcftools-cwl:1.12"

    input:
    tuple val(meta), path(joint_vcf_file)

    output:
    tuple val(meta), path("*.vcf.gz"), emit: split_vcf
    path("versions.yml")             , emit: versions

    script:
    """
    output_filename=\$(echo "${joint_vcf_file}" | sed "s|${meta.batch}|${meta.id}|1")

    bcftools view \\
        --output-type z \\
        --samples ${meta.id} \\
        --output "\${output_filename}" \\
        ${joint_vcf_file}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """

    stub:
    """
    output_filename=\$(echo "${joint_vcf_file}" | sed "s|${meta.batch}|${meta.id}|1")

    touch "\${output_filename}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """
}
