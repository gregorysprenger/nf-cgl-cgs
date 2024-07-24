/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { DEMULTIPLEX                 } from '../subworkflows/local/demultiplex'
include { DRAGEN_ALIGN                } from '../modules/local/dragen_align'
include { DRAGEN_JOINT_CNV            } from '../modules/local/dragen_joint_cnv'
include { DRAGEN_JOINT_SMALL_VARIANTS } from '../modules/local/dragen_joint_small_variants'
include { BCFTOOLS_SPLIT_VCF          } from '../modules/local/bcftools_split_vcf'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CREATE CHANNELS FOR INPUT PARAMETERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Illumina run directory
if (params.illumina_rundir) {
    ch_illumina_run_dir = Channel.fromPath(params.illumina_rundir, type: 'dir', checkIfExists: true)
} else {
    ch_illumina_run_dir = Channel.empty()
}

// Sample information
if (params.sample_info) {
    ch_sample_information = Channel.fromPath(params.sample_info, checkIfExists: true)
} else {
    ch_sample_information = Channel.empty()
}

// DRAGEN reference directory
if (params.refdir) {
    ch_reference_dir = Channel.fromPath(params.refdir, type: 'dir', checkIfExists: true)
} else {
    ch_reference_dir = []
}

// DRAGEN dbSNP annotation VCF
if (params.dbsnp) {
    ch_dbsnp_file = Channel.fromPath(params.dbsnp, checkIfExists: true)
} else {
    ch_dbsnp_file = []
}

// DRAGEN adapter sequences for read 1
if (params.adapter1) {
    ch_adapter1_file = Channel.fromPath(params.adapter1, checkIfExists: true)
} else {
    ch_adapter1_file = []
}

// DRAGEN adapter sequences for read 2
if (params.adapter2) {
    ch_adapter2_file = Channel.fromPath(params.adapter2, checkIfExists: true)
} else {
    ch_adapter2_file = []
}

// DRAGEN intermediate directory
if (params.intermediate_dir) {
    ch_intermediate_dir = Channel.fromPath(params.intermediate_dir)
} else {
    ch_intermediate_dir = []
}

// DRAGEN QC coverage over custom region
if (params.qc_coverage_region) {
    ch_qc_coverage_region = Channel.fromPath(params.qc_coverage_region)
} else {
    ch_qc_coverage_region = []
}

// DRAGEN QC cross-sample contamination
if (params.qc_cross_contamination) {
    ch_qc_cross_contamination = Channel.fromPath(params.qc_cross_contamination)
} else {
    ch_qc_cross_contamination = []
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow DRAGEN_CGS {

    take:
    ch_mgi_samplesheet    // channel: [ path(file) ]
    ch_samples            // channel: [ val(meta), path(file) ]

    main:
    ch_versions        = Channel.empty()
    ch_joint_vcf_files = Channel.empty()

    //
    // SUBWORKFLOW: Demultiplex samples
    //
    if (params.input && params.illumina_rundir && params.demux) {
        DEMULTIPLEX (
            ch_mgi_samplesheet,
            ch_illumina_run_dir.collect()
        )
        ch_versions = ch_versions.mix(DEMULTIPLEX.out.versions)

        ch_samples = DEMULTIPLEX.out.samples
    }

    if (params.sample_info) {
        ch_align_samples = ch_samples.map{
                                meta, fastq_list ->
                                    [ meta['acc'], meta['id'] ]
                            }
                            .join(
                                ch_sample_information
                                    .splitCsv( header: true )
                                    .map{ row -> [ row.Accession, row.gender ] },
                                remainder: true
                            )
                            .map{
                                acc, id, gender ->
                                    if (id) {
                                        def meta = [:]
                                        meta['id'] = id
                                        meta['acc'] = acc

                                        gender = gender ? gender.toLowerCase() : ""
                                        meta['sex'] = gender in ["male", "female"] ? gender :
                                                        (gender == "m") ? "male" :
                                                        (gender == "f") ? "female" : ""
                                        [ meta ]
                                    }
                            }
                            .combine(ch_samples.map{ meta, fastq_list -> fastq_list }.first())
    } else {
        ch_align_samples = ch_samples
    }

    //
    // MODULE: DRAGEN alignment
    //
    DRAGEN_ALIGN (
        ch_align_samples,
        ch_qc_cross_contamination.collect(),
        ch_qc_coverage_region.collect(),
        ch_intermediate_dir.collect(),
        ch_reference_dir.collect(),
        ch_adapter1_file.collect(),
        ch_adapter2_file.collect(),
        ch_dbsnp_file.collect()
    )
    ch_versions = ch_versions.mix(DRAGEN_ALIGN.out.versions)

    //
    // MODULE: Batch joint genotype CNV
    //
    if (params.joint_genotype_cnv) {
        DRAGEN_JOINT_CNV (
            DRAGEN_ALIGN.out.tangent_normalized_counts.collect(),
            ch_reference_dir.collect()
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_CNV.out.versions)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_CNV.out.joint_cnv)
    }

    //
    // MODULE: Batch joint genotype SNV/InDel
    //
    if (params.joint_genotype_small_variants) {
        DRAGEN_JOINT_SMALL_VARIANTS (
            DRAGEN_ALIGN.out.hard_filtered_gvcf.collect(),
            ch_reference_dir.collect()
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_SMALL_VARIANTS.out.versions)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_SMALL_VARIANTS.out.joint_small_variants)
    }

    //
    // MODULE: Batch joint genotype SV
    //
    if (params.joint_genotype_sv) {
        DRAGEN_JOINT_SV (
            DRAGEN_ALIGN.out.bam.collect(),
            ch_reference_dir.collect()
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_SV.out.versions)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_SV.out.joint_sv)
    }

    //
    // MODULE: Split joint genotyped VCF files by sample
    //
    BCFTOOLS_SPLIT_VCF (
        ch_joint_vcf_files
            .combine( ch_samples.map{ meta, file -> meta } )
            .map{
                vcf_meta, vcf, sample_meta ->
                    new_meta = sample_meta.clone()
                    new_meta['batch'] = vcf_meta['id']
                    [ new_meta, vcf ]
            }
    )
    ch_versions = ch_versions.mix(BCFTOOLS_SPLIT_VCF.out.versions)

    emit:
    versions = ch_versions
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
