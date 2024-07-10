/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { DRAGEN_ALIGN                } from '../modules/local/dragen_align'
include { DRAGEN_JOINT_CNV            } from '../modules/local/dragen_joint_cnv'
include { DRAGEN_JOINT_SMALL_VARIANTS } from '../modules/local/dragen_joint_small_variants'
include { BCFTOOLS_SPLIT_VCF          } from '../modules/local/bcftools_split_vcf'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow DRAGEN_CGS {

    take:
    ch_samples // channel: [ val(meta), path(file) ]

    main:
    ch_versions        = Channel.empty()
    ch_joint_vcf_files = Channel.empty()

    //
    // MODULE: DRAGEN alignment
    //
    DRAGEN_ALIGN (
        ch_samples
    )
    ch_versions = ch_versions.mix(DRAGEN_ALIGN.out.versions)

    //
    // MODULE: Batch joint genotype CNV
    //
    if (params.joint_genotype_cnv) {
        DRAGEN_JOINT_CNV (
            DRAGEN_ALIGN.out.tangent_normalized_counts.collect()
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_CNV.out.versions)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_CNV.out.joint_cnv)
    }

    //
    // MODULE: Batch joint genotype SNV/InDel
    //
    if (params.joint_genotype_small_variants) {
        DRAGEN_JOINT_SMALL_VARIANTS (
            DRAGEN_ALIGN.out.hard_filtered_gvcf.collect()
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_SMALL_VARIANTS.out.versions)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_SMALL_VARIANTS.out.joint_small_variants)
    }

    //
    // MODULE: Batch joint genotype SV
    //
    if (params.joint_genotype_sv) {
        DRAGEN_JOINT_SV (
            DRAGEN_ALIGN.out.bam.collect()
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
