/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { DRAGEN_JOINT_CNV            } from '../../modules/local/dragen_joint_cnv'
include { DRAGEN_JOINT_SMALL_VARIANTS } from '../../modules/local/dragen_joint_small_variants'
include { DRAGEN_JOINT_SV             } from '../../modules/local/dragen_joint_sv'
include { BCFTOOLS_SPLIT_VCF          } from '../../modules/local/bcftools_split_vcf'


// Save metric files to a directory
def saveMetricFile(channel, fileExt, outputDir) {
    channel.collectFile{
        sample, content ->
            new File("${outputDir}/${sample}").mkdirs()
            [ "${outputDir}/${sample}/${sample}.${fileExt}", content ]
    }
}

/*
========================================================================================
    SUBWORKFLOW TO JOINT GENOTYPE CNV, SNV/INDEL, SV
========================================================================================
*/

workflow JOINT_GENOTYPING {

    take:
    ch_dragen_output  // channel: [ path(file) ]
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
            ch_dragen_output.map{ it.findAll{ it.toString().endsWith('.tn.tsv.gz') } }.collect(),
            ch_reference_dir
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_CNV.out.versions)
        ch_dragen_usage    = ch_dragen_usage.mix(DRAGEN_JOINT_CNV.mix.usage)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_CNV.out.joint_cnv)

        // Replace lines in '<sample name>.cnv_metrics.csv' file
        // with values from joint called metrics
        ch_cnv_metrics = DRAGEN_JOINT_CNV.out.metrics.collect()
                            .combine(ch_dragen_output.map{ it.findAll{ it.toString().endsWith('cnv_metrics.csv') } })
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

        saveMetricFile(
            ch_cnv_metrics,
            'cnv_metrics.csv',
            "${params.outdir}/DRAGEN_output/"
        )

        ch_metric_files = ch_metric_files.mix(
                            saveMetricFile(
                                ch_cnv_metrics,
                                'cnv_metrics.csv',
                                "${workDir}/cnv_metrics"
                            )
                        )
    }

    //
    // MODULE: Batch joint genotype SNV/InDel
    //
    if (params.joint_genotype_small_variants) {
        DRAGEN_JOINT_SMALL_VARIANTS (
            ch_dragen_output.map{ it.findAll{ it.toString().endsWith(".hard-filtered.gvcf.gz") } }.collect(),
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
                                    .combine(ch_dragen_output.flatMap{ it.findAll{ it.toString().endsWith('vc_metrics.csv') } })
                                    .map{
                                        joint, sample ->
                                            def sample_name  = sample.getSimpleName().toString()
                                            def joint_lines  = joint.readLines()
                                            def sample_lines = sample.readLines()

                                            def indels_list = joint_sample_lines.findAll{
                                                                                    it.contains("Insertions") ||
                                                                                    it.contains("Deletions" ) ||
                                                                                    it.contains("Indels")
                                                                                }

                                            def indel_count   = indels_list.collect{ it.split(',')[3].toInteger() }.sum()
                                            def indel_percent = indels_list.collect{ it.split(',')[4].toFloat()   }.sum()

                                            def output = [
                                                joint_lines.find{  it =~ "Number of samples" },
                                                sample_lines.find{ it =~ "Reads Processed"   },
                                                sample_lines.find{ it =~ "Child Sample"      },
                                                joint_sample_lines,
                                                "JOINT CALLER POSTFILTER,${sample_name},Number of Indels,${indel_count},${indel_percent.round(2)}"
                                            ].flatten().findAll().join('\n')

                                            [ sample_name, output ]
                                    }

        saveMetricFile(
            ch_small_variant_metrics,
            'vc_metrics.csv',
            "${params.outdir}/DRAGEN_output/"
        )

        ch_metric_files = ch_metric_files.mix(
                            saveMetricFile(
                                ch_small_variant_metrics,
                                'vc_metrics.csv',
                                "${workDir}/small_variants_metrics"
                                )
                            )
    }

    //
    // MODULE: Batch joint genotype SV
    //
    if (params.joint_genotype_sv) {
        DRAGEN_JOINT_SV (
            ch_dragen_output.map{ it.findAll{ it.toString().endsWith(".bam") } }.collect(),
            ch_reference_dir
        )
        ch_versions        = ch_versions.mix(DRAGEN_JOINT_SV.out.versions)
        ch_dragen_usage    = ch_dragen_usage.mix(DRAGEN_JOINT_SV.out.usage)
        ch_joint_vcf_files = ch_joint_vcf_files.mix(DRAGEN_JOINT_SV.out.joint_sv)

        // Parse metrics for each sample from joint called '*.sv_metrics.csv' file
        ch_sv_metrics = DRAGEN_JOINT_SV.out.metrics
                            .splitText(elem: 0)
                            .map{ [ it.split(",")[1], it ] }

        saveMetricFile(
            ch_sv_metrics,
            'sv_metrics.csv',
            "${params.outdir}/DRAGEN_output/"
        )

        ch_metric_files = ch_metric_files.mix(
                            saveMetricFile(
                                ch_sv_metrics,
                                'sv_metrics.csv',
                                "${workDir}/sv_metrics"
                            )
                        )
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
    metric_files = ch_metric_files                   // channel: [ path(file) ]
    vcf_files    = BCFTOOLS_SPLIT_VCF.out.split_vcf  // channel: [ path(file) ]
    versions     = ch_versions                       // channel: [ path(file) ]

}
