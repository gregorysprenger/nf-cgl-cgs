process BCFTOOLS_SPLIT_VCF {
    tag "${meta.id}"
    label 'process_medium'

    container "mgibio/bcftools-cwl:1.12"

    input:
    tuple val(meta), path(joint_vcf_file)
    val(sample_name)

    output:
    path("*.vcf.gz")    , emit: split_vcf
    path("versions.yml"), emit: versions

    script:
    """
    # Replace meta.id in joint_vcf_file
    output_name=\$(echo "${joint_vcf_file}" | sed "s|${meta.id}|${sample_name}|1")

    bcftools view \\
        -O z \\
        -s "${sample_name}" \\
        -o "\${output_name}" \\
        "${joint_vcf_file}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """

    stub:
    """
    # Replace meta.id in joint_vcf_file
    output_name=\$(echo "${joint_vcf_file}" | sed "s|${meta.id}|${sample_name}|1")

    touch "\${output_name}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """
}
