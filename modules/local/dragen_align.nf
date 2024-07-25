process DRAGEN_ALIGN {
    tag "${meta.id}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el8.4.3.6' :
        'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    tuple val(meta), path(fastq_list)
    path(qc_cross_contamination_file)
    path(qc_coverage_region_file)
    path(intermediate_directory)
    path(reference_directory)
    path(adapter1_file)
    path(adapter2_file)
    path(dbsnp_file)

    output:
    tuple val(meta), path ("dragen/*")    , emit: dragen_output
    path("dragen/*.hard-filtered.gvcf.gz"), emit: hard_filtered_gvcf       , optional: true
    path("dragen/*.tn.tsv.gz")            , emit: tangent_normalized_counts, optional: true
    path("dragen/*.bam")                  , emit: bam
    path("versions.yml")                  , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args_license     = task.ext.dragen_license_args                  ?: ""
    def sample_sex       = meta.sex?.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                             : ""
    def dbsnp            = dbsnp_file                                    ? "--dbsnp ${dbsnp_file}"                                : ""
    def output_gvcf      = params.output_gvcf                            ? "--vc-emit-ref-confidence GVCF"                        : ""
    def ref_dir          = reference_directory                           ? "--ref-dir ${reference_directory}"                     : ""
    def adapter1         = adapter1_file                                 ? "--trim-adapter-read1 ${adapter1_file}"                : ""
    def adapter2         = adapter2_file                                 ? "--trim-adapter-read2 ${adapter2_file}"                : ""
    def qc_cont_vcf      = qc_cross_contamination_file                   ? "--qc-cross-cont-vcf ${qc_cross_contamination_file}"   : ""
    def qc_cov_region1   = qc_coverage_region_file                       ? "--qc-coverage-region-1 ${qc_coverage_region_file}"    : ""
    def intermediate_dir = intermediate_directory                        ? "--intermediate-results-dir ${intermediate_directory}" : ""

    """
    mkdir -p dragen

    /opt/dragen/4.3.6/bin/dragen \\
        --fastq-list ${fastq_list} \\
        --fastq-list-sample-id ${meta.id} \\
        --output-file-prefix ${meta.id} \\
        --output-directory dragen \\
        --force \\
        ${dbsnp} \\
        ${ref_dir} \\
        ${adapter1} \\
        ${adapter2} \\
        ${sample_sex} \\
        ${qc_cont_vcf} \\
        ${output_gvcf} \\
        ${args_license} \\
        --enable-sv true \\
        ${qc_cov_region1} \\
        --enable-cnv true \\
        --enable-sort true \\
        ${intermediate_dir} \\
        --output-format BAM \\
        --enable-map-align true \\
        --read-trimmers adapter \\
        --gc-metrics-enable true \\
        --sv-output-contigs true \\
        --enable-bam-indexing true \\
        --enable-variant-caller true \\
        --enable-map-align-output true \\
        --enable-duplicate-marking true \\
        --qc-coverage-ignore-overlaps true \\
        --cnv-enable-self-normalization true \\
        --variant-annotation-assembly GRCh38

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/dragen/4.3.6/bin/dragen --version | head -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version   = "4.3.6"
    def args_license     = task.ext.dragen_license_args                  ?: ""
    def sample_sex       = meta.sex?.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                             : ""
    def dbsnp            = dbsnp_file                                    ? "--dbsnp ${dbsnp_file}"                                : ""
    def output_gvcf      = params.output_gvcf                            ? "--vc-emit-ref-confidence GVCF"                        : ""
    def ref_dir          = reference_directory                           ? "--ref-dir ${reference_directory}"                     : ""
    def adapter1         = adapter1_file                                 ? "--trim-adapter-read1 ${adapter1_file}"                : ""
    def adapter2         = adapter2_file                                 ? "--trim-adapter-read2 ${adapter2_file}"                : ""
    def qc_cont_vcf      = qc_cross_contamination_file                   ? "--qc-cross-cont-vcf ${qc_cross_contamination_file}"   : ""
    def qc_cov_region1   = qc_coverage_region_file                       ? "--qc-coverage-region-1 ${qc_coverage_region_file}"    : ""
    def intermediate_dir = intermediate_directory                        ? "--intermediate-results-dir ${intermediate_directory}" : ""

    """
    mkdir -p dragen

    cat <<-END_CMDS > "dragen/${meta.id}.txt"
    /opt/dragen/4.3.6/bin/dragen \\
        --fastq-list ${fastq_list} \\
        --fastq-list-sample-id ${meta.id} \\
        --output-file-prefix ${meta.id} \\
        --output-directory dragen \\
        --force \\
        ${dbsnp} \\
        ${ref_dir} \\
        ${adapter1} \\
        ${adapter2} \\
        ${sample_sex} \\
        ${qc_cont_vcf} \\
        ${output_gvcf} \\
        ${args_license} \\
        --enable-sv true \\
        ${qc_cov_region1} \\
        --enable-cnv true \\
        --enable-sort true \\
        ${intermediate_dir} \\
        --output-format BAM \\
        --enable-map-align true \\
        --read-trimmers adapter \\
        --gc-metrics-enable true \\
        --sv-output-contigs true \\
        --enable-bam-indexing true \\
        --enable-variant-caller true \\
        --enable-map-align-output true \\
        --enable-duplicate-marking true \\
        --qc-coverage-ignore-overlaps true \\
        --cnv-enable-self-normalization true \\
        --variant-annotation-assembly GRCh38
    END_CMDS

    cp -rf ${projectDir}/assets/test_data/dragen_path/${meta.id}/* dragen/

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
