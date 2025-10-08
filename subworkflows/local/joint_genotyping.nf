/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { DRAGEN_JOINT_CNV            } from '../../modules/local/dragen_joint_cnv'
include { DRAGEN_JOINT_SMALL_VARIANTS } from '../../modules/local/dragen_joint_small_variants'
include { DRAGEN_JOINT_SV             } from '../../modules/local/dragen_joint_sv'
include { BCFTOOLS_SPLIT_VCF          } from '../../modules/local/bcftools_split_vcf'

/*
========================================================================================
    SUBWORKFLOW TO DEMULTIPLEX DATA
========================================================================================
*/

workflow JOINT_GENOTYPING {

    take:
    ch_dragen_output  // channel: [ val(meta), path(file) ]
    ch_reference_dir  // channel: [ path(file) ]

    main:
    ch_dragen_usage    = Channel.empty()
    ch_joint_vcf_files = Channel.empty()
    ch_metric_files    = Channel.empty()
    ch_versions        = Channel.empty()

    //
    // MODULE: Batch joint genotype CNV
    //
    if (params.joint_genotype_cnv) {
        DRAGEN_JOINT_CNV (
            ch_dragen_output.map{ meta, files -> files.findAll{ it.toString().endsWith('.tn.tsv.gz') } }.collect(),
            ch_reference_dir
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_CNV.out.versions)
        ch_dragen_usage    = ch_dragen_usage.mix(DRAGEN_JOINT_CNV.mix.usage)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_CNV.out.joint_cnv)

        // Replace lines in '<sample name>.cnv_metrics.csv' file
        // with values from joint called metrics
        ch_cnv_metrics = DRAGEN_JOINT_CNV.out.metrics.collect()
                            .combine(ch_dragen_output.map{ meta, files -> files.findAll{ it.toString().endsWith('cnv_metrics.csv') } })
                            .map{
                                joint, sample ->
                                    def joint_lines = joint.readLines()
                                    def sample_lines = sample.readLines()

                                    joint_lines.each{
                                        def pattern = it.split(',')[0..2].join(',')
                                        sample_lines.toString().replaceAll(("${pattern}.+"), (it)) as List
                                    }
                                    [ sample.getSimpleName(), sample_lines.join('\n') ]
                            }
                            .collectFile{
                                sample, output ->
                                    def outdir = file("${params.outdir}/DRAGEN_output/${sample}")
                                    outdir.mkdirs()
                                    [ "${outdir}/${sample}.cnv_metrics.csv", output ]
                            }

        ch_metric_files = ch_metric_files.mix(ch_cnv_metrics)
    }

    //
    // MODULE: Batch joint genotype SNV/InDel
    //
    if (params.joint_genotype_small_variants) {
        DRAGEN_JOINT_SMALL_VARIANTS (
            ch_dragen_output.map{ meta, files -> files.findAll{ it.toString().endsWith(".hard-filtered.gvcf.gz") } }.collect(),
            ch_reference_dir
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_SMALL_VARIANTS.out.versions)
        ch_dragen_usage    = ch_dragen_usage.mix(DRAGEN_JOINT_SMALL_VARIANTS.out.usage)
        ch_joint_vcf_files = ch_joint_vcf_files
                                .mix(
                                    DRAGEN_JOINT_SMALL_VARIANTS.out.joint_small_variants,
                                    DRAGEN_JOINT_SMALL_VARIANTS.out.joint_small_variants_filtered
                                )

        // Get values from single sample and joint called '*.vc_metrics.csv' files
        // and get all lines with sample name from joint called '*.vc_metrics.csv' file
        // and save to <sample name>.vc_metrics.csv file
        ch_small_variant_metrics = DRAGEN_JOINT_SMALL_VARIANTS.out.metrics.collect()
                                    .combine(ch_dragen_output.map{ meta, files -> files.findAll{ it.toString().endsWith('vc_metrics.csv') } })
                                    .map{
                                        joint, sample ->
                                            def sample_name = sample.getSimpleName()
                                            def joint_sample_lines = joint.text.findAll(".+${sample_name}.+")

                                            // Find values in joint vc_metrics.csv file
                                            def number_samples = joint.text.findAll("VARIANT CALLER SUMMARY,,Number of samples,.+")
                                            def indels_list = joint_sample_lines.findAll{
                                                                                    it.contains("Insertions") ||
                                                                                    it.contains("Deletions") ||
                                                                                    it.contains("Indels")
                                                                                }

                                            // Calculate number of indels
                                            def indel_count = 0
                                            def indel_percent = 0.0
                                            indels_list.each{
                                                indel_count += it.split(',')[3].toInteger()
                                                indel_percent += it.split(',')[4].toFloat()
                                            }

                                            def number_of_indels = ["JOINT CALLER POSTFILTER,${sample_name},Number of Indels,${indel_count},${indel_percent.round(2)}"]

                                            // Find values in single sample vc_metrics.csv file
                                            def reads_processed = sample.text.findAll("VARIANT CALLER SUMMARY,,Reads Processed,.+")
                                            def child_sample = sample.text.findAll("VARIANT CALLER SUMMARY,,Child Sample,.+")
                                            def autosome_callability = sample.text.findAll("VARIANT CALLER POSTFILTER,.+,Percent Autosome Callability,.+")[0]

                                            // Replace autosome callability in joint_sample_lines and create output
                                            def updated_lines = joint_sample_lines.collect{ it.replaceAll(/.+,Percent Autosome Callability,.+/, autosome_callability) }

                                            // Create output and return values
                                            def output = number_samples + reads_processed + child_sample + updated_lines + number_of_indels
                                            [ sample_name, output.join('\n') ]
                                    }
                                    .collectFile{
                                        sample, output ->
                                            def outdir = file("${params.outdir}/DRAGEN_output/${sample}")
                                            outdir.mkdirs()
                                            [ "${outdir}/${sample}.vc_metrics.csv", output ]
                                    }

        ch_metric_files = ch_metric_files.mix(ch_small_variant_metrics)
    }

    //
    // MODULE: Batch joint genotype SV
    //
    if (params.joint_genotype_sv) {
        DRAGEN_JOINT_SV (
            ch_dragen_output.map{ meta, files -> files.findAll{ it.toString().endsWith(".bam") } }.collect(),
            ch_reference_dir
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_SV.out.versions)
        ch_dragen_usage    = ch_dragen_usage.mix(DRAGEN_JOINT_SV.out.usage)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_SV.out.joint_sv)

        // Parse metrics for each sample
        // from joint called '*.sv_metrics.csv' file
        ch_sv_metrics = DRAGEN_JOINT_SV.out.metrics
                            .splitText(elem: 0)
                            .collectFile{
                                def sample = it.split(",")[1]
                                def outdir = file("${params.outdir}/${sample}")
                                outdir.mkdirs()
                                [ "${outdir}/${sample}.sv_metrics.csv", it ]
                            }

        ch_metric_files = ch_metric_files.mix(ch_sv_metrics)
    }

    //
    // MODULE: Split joint genotyped VCF files by sample
    //
    BCFTOOLS_SPLIT_VCF (
        ch_joint_vcf_files
    )
    ch_versions = ch_versions.mix(BCFTOOLS_SPLIT_VCF.out.versions)

    emit:
    dragen_usage = ch_dragen_usage                   // channel: [ path(file) ]
    metrics      = ch_metric_files                   // channel: [ path(file) ]
    vcf_files    = BCFTOOLS_SPLIT_VCF.out.split_vcf  // channel: [ path(file) ]
    versions     = ch_versions                       // channel: [ path(file) ]

}
