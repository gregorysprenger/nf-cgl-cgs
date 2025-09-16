/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { DEMULTIPLEX                          } from '../subworkflows/local/demultiplex'
include { DRAGEN_ALIGN                         } from '../modules/local/dragen_align'
include { DRAGEN_ALIGN as DRAGEN_ALIGN_CONTROL } from '../modules/local/dragen_align'
include { JOINT_GENOTYPING                     } from '../subworkflows/local/joint_genotyping'
include { PARSE_QC_METRICS                     } from '../modules/local/parse_qc_metrics'
include { TRANSFER_DATA_AWS                    } from '../modules/local/transfer_data_aws'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CREATE CHANNELS FOR INPUT PARAMETERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Original input file - has not gone through XLSX -> CSV conversion
if (params.input) {
    ch_input_file = Channel.fromPath(params.input).collect()
} else {
    ch_input_file = Channel.empty()
}
// Sample information
if (params.sample_info) {
    ch_sample_information = Channel.fromPath(params.sample_info, checkIfExists: true).collect()
} else {
    ch_sample_information = Channel.empty()
}

// DRAGEN reference directory
if (params.refdir) {
    ch_reference_dir = Channel.fromPath(params.refdir, type: 'dir', checkIfExists: true).collect()
} else {
    ch_reference_dir = []
}

// DRAGEN dbSNP annotation VCF
if (params.dbsnp) {
    ch_dbsnp_file = Channel.fromPath(params.dbsnp, checkIfExists: true).collect()
} else {
    ch_dbsnp_file = []
}

// DRAGEN adapter sequences for read 1
if (params.adapter1) {
    ch_adapter1_file = Channel.fromPath(params.adapter1, checkIfExists: true).collect()
} else {
    ch_adapter1_file = []
}

// DRAGEN adapter sequences for read 2
if (params.adapter2) {
    ch_adapter2_file = Channel.fromPath(params.adapter2, checkIfExists: true).collect()
} else {
    ch_adapter2_file = []
}

// DRAGEN intermediate directory
if (params.intermediate_dir?.toString()?.startsWith('/staging')) {
    ch_intermediate_dir = Channel.of(params.intermediate_dir).map{ [ it, [] ] }.collect()
} else if (params.intermediate_dir) {
    ch_intermediate_dir = Channel.fromPath(params.intermediate_dir).map{ [ [], it ] }.collect()
} else {
    ch_intermediate_dir = [ [], [] ]
}

// DRAGEN QC coverage over custom region
if (params.qc_coverage_region) {
    ch_qc_coverage_region = Channel.fromPath(params.qc_coverage_region).collect()
} else {
    ch_qc_coverage_region = []
}

// DRAGEN QC cross-sample contamination
if (params.qc_cross_contamination?.toString()?.startsWith('resources/')) {
    ch_qc_cross_contamination = Channel.of(params.qc_cross_contamination).map{ [ it, [] ] }.collect()
} else if (params.qc_cross_contamination) {
    ch_qc_cross_contamination = Channel.fromPath(params.qc_cross_contamination).map{ [ [], it ] }.collect()
} else {
    ch_qc_cross_contamination = [ [], [] ]
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow DRAGEN_CGS {

    take:
    ch_samplesheet  // channel: [ path(file) ]
    ch_samples      // channel: [ val(meta), path(file) ]

    main:
    ch_versions       = Channel.empty()
    ch_dragen_usage   = Channel.empty()
    ch_dragen_metrics = Channel.empty()

    //
    // SUBWORKFLOW: Demultiplex samples
    //
    if (params.input && params.illumina_rundir) {
        DEMULTIPLEX (
            ch_samplesheet
        )
        ch_versions = ch_versions.mix(DEMULTIPLEX.out.versions)
        ch_samples  = ch_samples.mix(DEMULTIPLEX.out.samples)
    }

    // Fetch gender for samples
    if (params.sample_info) {
        ch_samples = ch_samples
                        .map{ meta, reads, fastq_list -> [ meta?.acc ?: meta?.id, meta, reads, fastq_list ] }
                        .join(
                            ch_sample_information
                                .splitCsv(header: true)
                                .map{ [ it['Accession'].join(''), it['gender'].join('').toLowerCase() ] },
                            remainder: true
                        )
                        .filter{ it[1] != null }
                        .map{
                            acc, meta, reads, fastq_list, gender ->
                                if (meta) {
                                    def meta_new = meta.clone()
                                    meta_new['sex']   = (gender == 'm' || gender == 'male')   ? 'male'   :
                                                        (gender == 'f' || gender == 'female') ? 'female' : ''

                                    [ meta_new, reads, fastq_list ]
                                }
                        }
    }

    // Automatically determine if GVCF should be created
    ch_samples = ch_samples
                    .combine(ch_samples.count().map{ it > 1 && params.create_gvcf })
                    .map{
                        meta, reads, fastq_list, create_gvcf ->
                            def meta_new = meta.clone()
                            meta_new['create_gvcf'] = create_gvcf

                            [ meta_new, reads, fastq_list ]
                    }

    // Verify no duplicate samples exist
    ch_samples
        .map{ meta, reads, fastq_list -> [ reads ] }
        .collect()
        .map{
            def duplicates = it.findAll{ sample -> it.count(sample) > 1 }.unique()
            if (duplicates) {
                error "Duplicate entries found in channel:\n${duplicates.flatten()}"
            }
        }

    //
    // MODULE: DRAGEN alignment for clinical samples
    //
    DRAGEN_ALIGN (
        ch_samples.filter{ meta, reads, fastq_list -> params.validation_samples || meta?.acc.startsWith("G") },
        ch_intermediate_dir,
        ch_qc_cross_contamination,
        ch_adapter1_file,
        ch_adapter2_file,
        ch_dbsnp_file,
        ch_qc_coverage_region,
        ch_reference_dir
    )
    ch_versions     = ch_versions.mix(DRAGEN_ALIGN.out.versions)
    ch_dragen_usage = ch_dragen_usage.mix(DRAGEN_ALIGN.out.usage)
    ch_dragen_metrics = ch_dragen_metrics.mix(DRAGEN_ALIGN.out.metrics)

    //
    // SUBWORKFLOW: Joint genotyping
    //
    JOINT_GENOTYPING (
        DRAGEN_ALIGN.out.dragen_output.map{ meta, files -> files },
        ch_reference_dir
    )
    ch_versions     = ch_versions.mix(JOINT_GENOTYPING.out.versions)
    ch_dragen_usage = ch_dragen_usage.mix(JOINT_GENOTYPING.out.dragen_usage)

    if (!params.validation_samples) {
        //
        // MODULE: DRAGEN alignment for control samples
        //
        DRAGEN_ALIGN_CONTROL (
            JOINT_GENOTYPING.out.dragen_usage
                    .collect()
                    .combine(ch_samples.filter{ meta, reads, fastq_list -> !meta?.acc.startsWith("G") })
                    .map{
                        done, meta, reads, fastq_list ->
                            meta['create_gvcf'] = false
                            [ meta, reads, fastq_list ]
                    },
            ch_intermediate_dir,
            ch_qc_cross_contamination,
            ch_adapter1_file,
            ch_adapter2_file,
            ch_dbsnp_file,
            ch_qc_coverage_region,
            ch_reference_dir
        )
        ch_versions     = ch_versions.mix(DRAGEN_ALIGN_CONTROL.out.versions)
        ch_dragen_usage = ch_dragen_usage.mix(DRAGEN_ALIGN_CONTROL.out.usage)
        ch_dragen_metrics = ch_dragen_metrics.mix(DRAGEN_ALIGN_CONTROL.out.metrics)
    }

    //
    // MODULE: Parse QC metrics
    //
    PARSE_QC_METRICS (
        ch_input_file.ifEmpty([]),
        ch_dragen_metrics.collect(),
        JOINT_GENOTYPING.out.metrics.collect().ifEmpty([])
    )
    ch_versions = ch_versions.mix(PARSE_QC_METRICS.out.versions)

    // Transfer data to AWS bucket
    if (params.transfer_data) {
        ch_upload_files = DRAGEN_ALIGN.out.dragen_output
            .map{
                meta, files ->
                    def extensions = [
                        ".bam",
                        ".bed",
                        ".bw",
                        ".csv",
                        ".gff3",
                        ".json",
                        ".vcf",
                    ]
                    return files.findAll{ file -> extensions.any{ file.toString().toLowerCase().contains(it) } }
            }
            .collect()
            .toList()
            .combine(JOINT_GENOTYPING.out.vcf_files.collect().ifEmpty([]).toList())
            .combine(JOINT_GENOTYPING.out.metrics.collect().ifEmpty([]).toList())
            .map{
                dragen, vcf, metrics ->
                    def exclude_filenames = (vcf + metrics).collect { it.name }
                    dragen.findAll { !exclude_filenames.contains(it.name) } + vcf + metrics
            }
            .view()

        //
        // MODULE: Transfer data to AWS bucket
        //
        TRANSFER_DATA_AWS (
            ch_upload_files,
            PARSE_QC_METRICS.out.genoox_metrics
        )
        ch_versions = ch_versions.mix(TRANSFER_DATA_AWS.out.versions)
    }

    // Output DRAGEN usage information
    ch_dragen_usage.map{
                        def meta = it.getSimpleName().split("_usage")[0]
                        def data = it.text.split("\\: ").join('\t')
                        return "Accession\tLicense Type\tUsage\n${meta}\t${data}"
                    }
                    .collectFile(
                        name      : "DRAGEN_usage.tsv",
                        keepHeader: true,
                        storeDir  : "${params.outdir}/pipeline_info"
                    )

    emit:
    versions = ch_versions

}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
