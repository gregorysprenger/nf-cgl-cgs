process DRAGEN_ALIGN {
    tag "${meta.id}"
    label 'dragen'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el8.4.3.6' :
        'dockerreg01.accounts.ad.wustl.edu/cgl/dragen:v4.3.6' }"

    input:
    tuple val(meta), path(fastq_list)

    output:
    tuple val(meta), path ("dragen/*")    , emit: dragen_output
    path("dragen/*.hard-filtered.gvcf.gz"), emit: hard_filtered_gvcf
    path("dragen/*.tn.tsv.gz")            , emit: tangent_normalized_counts
    path("dragen/*.bam")                  , emit: bam
    path("versions.yml")                  , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args_license     = task.ext.dragen_license_args                 ?: ""
    def sample_sex       = meta.sex.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                                 : ""
    def dbsnp            = params.dbsnp                                 ? "--dbsnp ${params.dbsnp}"                                  : ""
    def ref_dir          = params.refdir                                ? "--ref-dir ${params.refdir}"                               : ""
    def adapter1         = params.adapter1                              ? "--trim-adapter-read1 ${params.adapter1}"                  : ""
    def adapter2         = params.adapter2                              ? "--trim-adapter-read2 ${params.adapter2}"                  : ""
    def qc_cont_vcf      = params.qc_cross_contamination_vcf            ? "--qc-cross-cont-vcf ${params.qc_cross_contamination_vcf}" : ""
    def qc_cov_region1   = params.qc_coverage_region_1                  ? "--qc-coverage-region-1 ${params.qc_coverage_region_1}"    : ""
    def intermediate_dir = params.intermediate_dir                      ? "--intermediate-results-dir ${params.intermediate_dir}"    : ""

    """
    mkdir -p dragen

    /opt/edico/bin/dragen \\
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
        --vc-emit-ref-confidence GVCF \\
        --enable-map-align-output true \\
        --enable-duplicate-marking true \\
        --qc-coverage-ignore-overlaps true \\
        --cnv-enable-self-normalization true

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def dragen_version   = "4.2.4"
    def args_license     = task.ext.dragen_license_args                 ?: ""
    def sample_sex       = meta.sex.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                                 : ""
    def dbsnp            = params.dbsnp                                 ? "--dbsnp ${params.dbsnp}"                                  : ""
    def ref_dir          = params.refdir                                ? "--ref-dir ${params.refdir}"                               : ""
    def adapter1         = params.adapter1                              ? "--trim-adapter-read1 ${params.adapter1}"                  : ""
    def adapter2         = params.adapter2                              ? "--trim-adapter-read2 ${params.adapter2}"                  : ""
    def qc_cont_vcf      = params.qc_cross_contamination_vcf            ? "--qc-cross-cont-vcf ${params.qc_cross_contamination_vcf}" : ""
    def qc_cov_region1   = params.qc_coverage_region_1                  ? "--qc-coverage-region-1 ${params.qc_coverage_region_1}"    : ""
    def intermediate_dir = params.intermediate_dir                      ? "--intermediate-results-dir ${params.intermediate_dir}"    : ""

    """
    mkdir -p dragen

    cat <<-END_CMDS > "dragen/${meta.id}.txt"
    /opt/edico/bin/dragen \\
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
        --vc-emit-ref-confidence GVCF \\
        --enable-map-align-output true \\
        --enable-duplicate-marking true \\
        --qc-coverage-ignore-overlaps true \\
        --cnv-enable-self-normalization true
    END_CMDS

    cp -rf ${projectDir}/assets/test_data/dragen_path/${meta.id}/* dragen/

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
