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

    process_vcf() {
        local vcf_file="\$1"
        local base=\$(basename "\$vcf_file" .vcf)
        local ext=\$(echo "${joint_vcf_file}" | sed "s|${prefix.id}||1")
        local out="split_vcf/\${base}\${ext}"

        # Filter out homozygous reference calls and compress
        bcftools view \\
            -i 'GT!="." && GT!="0/0"' \\
            -Oz \\
            -o "\$out" \\
            "\$vcf_file"

        # Remove the original uncompressed VCF
        rm "\$vcf_file"

        # Index the VCF file
        bcftools index --tbi "\$out"

        # Create an MD5 checksum for the new file
        md5sum "\$out" | sed "s|split_vcf/||g" > "\${out}.md5sum"
    }
    export -f process_vcf

    # Split joint VCF file into individual sample VCF files
    bcftools +split \\
        --output-type v \\
        --output split_vcf/ \\
        ${joint_vcf_file}

    # Process split VCF files in parallel using the function
    find split_vcf/ -name "*.vcf" \\
        | xargs -P ${task.cpus} -I {} bash -c 'process_vcf "{}"'

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix
    """
    mkdir -p split_vcf
    ext=\$(echo "${joint_vcf_file}" | sed "s|${prefix.id}||1")
    for sample in \$(bcftools query -l ${joint_vcf_file}); do
        touch "split_vcf/\${sample}\${ext}" \\
            "split_vcf/\${sample}\${ext}.tbi"
        touch "split_vcf/\${sample}\${ext}.md5sum"
    done

    cat <<-END_CMDS > ${prefix.id}_cmds.txt
    process_vcf() {
        local vcf_file="\$1"
        local base=\$(basename "\$vcf_file" .vcf)
        local ext=\$(echo "${joint_vcf_file}" | sed "s|${prefix.id}||1")
        local out="split_vcf/\${base}\${ext}"

        # Filter out homozygous reference calls and compress
        bcftools view \\
            -i 'GT!="." && GT!="0/0"' \\
            -Oz \\
            -o "\$out" \\
            "\$vcf_file"

        # Remove the original uncompressed VCF
        rm "\$vcf_file"

        # Index the VCF file
        bcftools index --tbi "\$out"

        # Create an MD5 checksum for the new file
        md5sum "\$out" | sed "s|split_vcf/||g" > "\${out}.md5sum"
    }
    export -f process_vcf

    # Split joint VCF file into individual sample VCF files
    bcftools +split \\
        --output-type v \\
        --output split_vcf/ \\
        ${joint_vcf_file}

    # Process split VCF files in parallel using the function
    find split_vcf/ -name "*.vcf" \\
        | xargs -P ${task.cpus} -I {} bash -c 'process_vcf "{}"'
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bcftools: \$(bcftools --version | head -n 1 | cut -d ' ' -f2)
    END_VERSIONS
    """
}
