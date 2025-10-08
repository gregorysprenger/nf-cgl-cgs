process BCFTOOLS_SPLIT_VCF {
    tag "${joint_vcf_file}"
    label 'process_medium'

    container "docker.io/mgibio/bcftools-cwl:1.12"

    input:
    path(joint_vcf_file)

    output:
    path("split_vcf/*") , emit: split_vcf
    path("versions.yml"), emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix
    """
    mkdir -p split_vcf

    # Split joint VCF file into individual sample VCF files
    bcftools +split \\
        --output-type v \\
        --output split_vcf/ \\
        ${joint_vcf_file}

    # Get the file extension from the joint VCF file
    ext=\$(echo "${joint_vcf_file}" | sed "s|${prefix.id}||1")

    # Compress and index each VCF file, then create MD5SUM
    find split_vcf/ -name "*.vcf" \\
        | xargs -I {} -P ${task.cpus} bash -c '
            base=\$(basename "\${0}" .vcf)
            out="split_vcf/\${base}\${ext}"

            bcftools view -Oz -o "\${out}" "\${0}"
            rm "\${0}"

            bcftools index --tbi "\${out}"

            md5sum "\${out}" | sed "s|split_vcf/||g" > "\${out}.md5sum"
        ' _ {} \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix
    """
    mkdir -p split_vcf

    # Get the file extension from the joint VCF file
    ext=\$(echo "${joint_vcf_file}" | sed "s|${prefix.id}||1")

    for sample in \$(bcftools query -l ${joint_vcf_file}); do
        touch "split_vcf/\${sample}\${ext}" \\
            "split_vcf/\${sample}\${ext}.tbi"
    done

    cat <<-END_CMDS > ${prefix.id}_cmds.txt
    # Split joint VCF file into individual sample VCF files
    bcftools +split \\
        --output-type v \\
        --output split_vcf/ \\
        ${joint_vcf_file}

    # Compress and index each VCF file, then create MD5SUM
    find split_vcf/ -name "*.vcf" \\
        | xargs -I {} -P ${task.cpus} bash -c '
            base=\$(basename "\${0}" .vcf)
            out="split_vcf/\${base}\${ext}"

            bcftools view -Oz -o "\${out}" "\${0}"
            rm "\${0}"

            bcftools index --tbi "\${out}"

            md5sum "\${out}" | sed "s|split_vcf/||g" > "\${out}.md5sum"
        ' _ {} \;
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """
}
