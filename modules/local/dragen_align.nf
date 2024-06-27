process DRAGEN_ALIGN {
    tag "${meta.id}"
    label 'dragen'
    label 'dragenalign'

    container "${ workflow.profile == 'dragenaws' ?
        'ghcr.io/dhslab/docker-dragen:el7.4.2.4' :
        'etycksen/dragen4:4.2.4' }"

    input:
    tuple val(meta), val(type), path("*")
    tuple val(dragen_inputs), path("*", stageAs: 'inputs/*')

    output:
    tuple val(meta), path ("dragen/*"), emit: dragen_output
    path("versions.yml")              , emit: versions

    script:
    def input = ""
    if (type == 'fastq_list') {
        input = "--fastq-list fastq_list.csv --fastq-list-sample-id ${meta.id}"
    } else if (type == 'cram') {
        input = "--cram-input ${meta.cram}"
    } else if (type == 'bam') {
        input = "--bam-input ${meta.bam}"
    }

    def args_license     = task.ext.dragen_license_args                 ?: ''
    def sample_sex       = meta.sex.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                                     : ""
    def dbsnp            = params.dragen_dbsnp                          ? "--dbsnp ${params.dragen_dbsnp}"                               : ""
    def ref_dir          = params.dragen_ref_dir                        ? "--ref-dir ${params.dragen_ref_dir}"                           : ""
    def adapter1         = params.dragen_adatper1                       ? "--trim-adapter-read1 ${params.dragen_adatper1}"               : ""
    def adapter2         = params.dragen_adapter2                       ? "--trim-adapter-read2 ${params.dragen_adatper2}"               : ""
    def qc_cont_vcf      = params.qc_contamination_vcf                  ? "--qc-cross-cont-vcf ${params.qc_contamination_vcf}"           : ""
    def qc_cov_region1   = params.qc_coverage_region_1                  ? "--qc-coverage-region-1 ${params.qc_coverage_region_1}"        : ""
    def intermediate_dir = params.dragen_intermediate_dir               ? "--intermediate-results-dir ${params.dragen_intermediate_dir}" : ""
    """
    mkdir -p dragen

    /opt/edico/bin/dragen \\
        ${input} \\
        ${dbsnp} \\
        ${ref_dir} \\
        ${adapter1} \\
        ${adapter2} \\
        ${sample_sex} \\
        ${qc_cont_vcf} \\
        ${qc_cov_region1}
        ${args_license} \\
        ${intermediate_dir} \\
        --output-file-prefix ${meta.id} \\
        --force \\
        --enable-sv true \\
        --enable-cnv true \\
        --enable-sort true \\
        --output-format BAM \\
        --enable-map-align true \\
        --read-trimmers adapter \\
        --gc-metrics-enable true \\
        --sv-output-contigs true \\
        --output-directory dragen \\
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
    def input = ""
    if (type == 'fastq_list') {
        input = "--fastq-list fastq_list.csv --fastq-list-sample-id ${meta.id}"

    } else if (type == 'cram') {
        input = "--cram-input ${meta.cram}"
    }
    if (type == 'bam') {
        input = "--bam-input ${meta.bam}"
    }

    def dragen_version   = "4.2.4"
    def args_license     = task.ext.dragen_license_args                 ?: ''
    def sample_sex       = meta.sex.toLowerCase() in ['male', 'female'] ? "--sample-sex ${meta.sex}"                              : ""
    def dbsnp            = params.dragen_dbsnp                          ? "--dbsnp ${params.dragen_dbsnp}"                        : ""
    def ref_dir          = params.dragen_ref_dir                        ? "--ref-dir ${params.dragen_ref_dir}"                    : ""
    def adapter1         = params.dragen_adatper1                       ? "--trim-adapter-read1 ${params.dragen_adatper1}"        : ""
    def adapter2         = params.dragen_adapter2                       ? "--trim-adapter-read2 ${params.dragen_adatper2}"        : ""
    def qc_cont_vcf      = params.qc_contamination_vcf                  ? "--qc-cross-cont-vcf ${params.qc_contamination_vcf}"    : ""
    def qc_cov_region1   = params.qc_coverage_region_1                  ? "--qc-coverage-region-1 ${params.qc_coverage_region_1}" : ""
    def intermediate_dir = params.dragen_intermediate_dir               ? "--intermediate-results-dir ${dragen_intermediate_dir}" : ""
    """
    mkdir -p dragen

    cat <<-END_CMDS > "dragen/${meta.id}.txt"
    /opt/edico/bin/dragen \\
        ${input} \\
        ${dbsnp} \\
        ${ref_dir} \\
        ${adapter1} \\
        ${adapter2} \\
        ${sample_sex} \\
        ${qc_cont_vcf} \\
        ${qc_cov_region1}
        ${args_license} \\
        ${intermediate_dir} \\
        --output-file-prefix ${meta.id} \\
        --force \\
        --enable-sv true \\
        --enable-cnv true \\
        --enable-sort true \\
        --output-format BAM \\
        --enable-map-align true \\
        --read-trimmers adapter \\
        --gc-metrics-enable true \\
        --sv-output-contigs true \\
        --output-directory dragen \\
        --enable-bam-indexing true \\
        --enable-variant-caller true \\
        --vc-emit-ref-confidence GVCF \\
        --enable-map-align-output true \\
        --enable-duplicate-marking true \\
        --qc-coverage-ignore-overlaps true \\
        --cnv-enable-self-normalization true
    END_CMDS

    for i in ${projectDir}/assets/stub/dragen_path/*; do
        cp "\${i}" ./dragen/
    done

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: ${dragen_version}
    END_VERSIONS
    """
}
