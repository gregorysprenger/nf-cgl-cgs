process DRAGEN_ALIGN {
    tag "${meta.id}"
    label 'dragen'

    container "${ ['awsbatch','dragenaws'].any{ workflow.profile.contains(it) }
        ? 'job-definition://dragen_v4-3-6'
        : 'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    tuple val(meta), path(reads, stageAs: "fastq_files/*"), path(fastq_list), path(alignment_file)
    tuple val(intermediate_directory_value), path(intermediate_directory)
    tuple val(qc_contamination_value)      , path(qc_contamination_file)
    path(adapter1_file)
    path(adapter2_file)
    path(dbsnp_file)
    path(qc_coverage_region_file)
    path(reference_directory)
    path(cram_reference_file)

    output:
    tuple val(meta), path("${meta.id}/*")  , emit: dragen_output
    path("${meta.id}/${meta.id}_usage.txt"), emit: usage        , optional: true
    path("${meta.id}/*_metrics.csv")       , emit: metrics
    path("versions.yml")                   , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def exe_path = ['awsbatch','dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"

    def input
    if (alignment_file && alignment_file.name.toLowerCase().endsWith('.bam')) {
        input = "--bam-input ${alignment_file}"
    } else if (alignment_file && alignment_file.name.toLowerCase().endsWith('.cram')) {
        input = "--cram-input ${alignment_file}"
    } else if (fastq_list && fastq_list.name.toLowerCase().endsWith('.csv')) {
        input = "--fastq-list ${fastq_list} --fastq-list-sample-id ${meta.id}"
    } else {
        error("Input file is not a BAM, CRAM, or CSV file.")
    }

    def cram_ref = cram_reference_file ? cram_reference_file.find{
                        def pathStr = it.toString().toLowerCase()
                        pathStr.endsWith('.fa') || pathStr.endsWith('.fasta')
                    } : []

    def alignment_args = [
        task.ext.dragen_license_args                  ?: "",
        meta.sex?.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                                   : "",
        dbsnp_file                                    ? "--dbsnp ${dbsnp_file}"                                      : "",
        meta.create_gvcf                              ? "--vc-emit-ref-confidence GVCF"                              : "",
        reference_directory                           ? "--ref-dir ${reference_directory}"                           : "",
        cram_ref                                      ? "--cram-reference ${cram_ref}"                               : "",
        adapter1_file                                 ? "--trim-adapter-read1 ${adapter1_file}"                      : "",
        adapter2_file                                 ? "--trim-adapter-read2 ${adapter2_file}"                      : "",
        qc_contamination_file                         ? "--qc-cross-cont-vcf ${qc_contamination_file}"               : "",
        qc_contamination_value                        ? "--qc-cross-cont-vcf ${exe_path}/${qc_contamination_value}"  : "",
        qc_coverage_region_file                       ? "--qc-coverage-region-1 ${qc_coverage_region_file}"          : "",
        intermediate_directory                        ? "--intermediate-results-dir ${intermediate_directory}"       : "",
        intermediate_directory_value                  ? "--intermediate-results-dir ${intermediate_directory_value}" : ""
    ].join(' ').trim()
    """
    mkdir -p ${meta.id}

    ${exe_path}/bin/dragen \\
        ${input} \\
        ${alignment_args} \\
        --cnv-enable-self-normalization true \\
        --enable-bam-indexing true \\
        --enable-cnv true \\
        --enable-duplicate-marking true \\
        --enable-map-align true \\
        --enable-map-align-output true \\
        --enable-sort true \\
        --enable-sv true \\
        --enable-variant-caller true \\
        --force \\
        --gc-metrics-enable true \\
        --output-directory ${meta.id} \\
        --output-file-prefix ${meta.id} \\
        --output-format BAM \\
        --qc-coverage-ignore-overlaps true \\
        --read-trimmers adapter \\
        --sv-output-contigs true \\
        --variant-annotation-assembly GRCh38

    # Create md5sum for files
    find ${meta.id}/ \\
        -type f \\
        ! -name "*.md5sum" \\
        -exec bash -c 'md5sum "{}" | sed "s| .*/| |" > "{}.md5sum"' \\;

    # Copy and rename DRAGEN usage
    find ${meta.id}/ \\
        -type f \\
        -name "*_usage.txt" \\
        -exec mv "{}" "${meta.id}/${meta.id}_usage.txt" \\;

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def exe_path = ['awsbatch','dragenaws'].any{ workflow.profile.contains(it) } ? "/opt/edico" : "/opt/dragen/4.3.6"

    def input
    if (alignment_file && alignment_file.name.endsWith('.bam')) {
        input = "--bam-input ${alignment_file}"
    } else if (alignment_file && alignment_file.name.endsWith('.cram')) {
        input = "--cram-input ${alignment_file}"
    } else if (fastq_list && fastq_list.name.endsWith('.csv')) {
        input = "--fastq-list ${fastq_list} --fastq-list-sample-id ${meta.id}"
    } else {
        error("Input file is not a BAM, CRAM, or CSV file.")
    }

    def cram_ref = cram_reference_file ? cram_reference_file.find{ def s = it.toString().toLowerCase(); s.endsWith('.fa') || s.endsWith('.fasta') } : []

    def alignment_args = [
        task.ext.dragen_license_args                  ?: "",
        meta.sex?.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                                   : "",
        dbsnp_file                                    ? "--dbsnp ${dbsnp_file}"                                      : "",
        meta.create_gvcf                              ? "--vc-emit-ref-confidence GVCF"                              : "",
        reference_directory                           ? "--ref-dir ${reference_directory}"                           : "",
        cram_ref                                      ? "--cram-reference ${cram_ref}"                               : "",
        adapter1_file                                 ? "--trim-adapter-read1 ${adapter1_file}"                      : "",
        adapter2_file                                 ? "--trim-adapter-read2 ${adapter2_file}"                      : "",
        qc_contamination_file                         ? "--qc-cross-cont-vcf ${qc_contamination_file}"               : "",
        qc_contamination_value                        ? "--qc-cross-cont-vcf ${exe_path}/${qc_contamination_value}"  : "",
        qc_coverage_region_file                       ? "--qc-coverage-region-1 ${qc_coverage_region_file}"          : "",
        intermediate_directory                        ? "--intermediate-results-dir ${intermediate_directory}"       : "",
        intermediate_directory_value                  ? "--intermediate-results-dir ${intermediate_directory_value}" : ""
    ].join(' ').trim()
    """
    mkdir -p ${meta.id}

    touch \\
        "${meta.id}/${meta.id}.bam" \\
        "${meta.id}/${meta.id}.tn.tsv.gz" \\
        "${meta.id}/${meta.id}_metrics.csv" \\
        "${meta.id}/${meta.id}_usage.txt" \\
        "${meta.id}/${meta.id}.hard-filtered.gvcf.gz"

    find ${meta.id}/ \\
        -type f \\
        ! -name "*.md5sum" \\
        -exec bash -c 'md5sum "{}" | sed "s| .*/| |" > "{}.md5sum"' \\;

    cat <<-END_CMDS > "${meta.id}/${meta.id}.txt"
    ${exe_path}/bin/dragen \\
        ${input} \\
        ${alignment_args} \\
        --cnv-enable-self-normalization true \\
        --enable-bam-indexing true \\
        --enable-cnv true \\
        --enable-duplicate-marking true \\
        --enable-map-align true \\
        --enable-map-align-output true \\
        --enable-sort true \\
        --enable-sv true \\
        --enable-variant-caller true \\
        --force \\
        --gc-metrics-enable true \\
        --output-directory ${meta.id} \\
        --output-file-prefix ${meta.id} \\
        --output-format BAM \\
        --qc-coverage-ignore-overlaps true \\
        --read-trimmers adapter \\
        --sv-output-contigs true \\
        --variant-annotation-assembly GRCh38

    # Create md5sum for files
    find ${meta.id}/ \\
        -type f \\
        ! -name "*.md5sum" \\
        -exec bash -c 'md5sum "{}" | sed "s| .*/| |" > "{}.md5sum"' \\;

    # Copy and rename DRAGEN usage
    find ${meta.id}/ \\
        -type f \\
        -name "*_usage.txt" \\
        -exec mv "{}" "${meta.id}/${meta.id}_usage.txt" \\;
    END_CMDS

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(${exe_path}/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """
}
