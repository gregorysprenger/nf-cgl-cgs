include { DRAGEN_MULTIALIGN as DRAGEN_FASTQ_LIST    } from '../../modules/local/dragen_multialign.nf'
include { DRAGEN_MULTIALIGN as DRAGEN_CRAM          } from '../../modules/local/dragen_multialign.nf'
include { DRAGEN_MULTIALIGN as DRAGEN_BAM           } from '../../modules/local/dragen_multialign.nf'

workflow GERMLINE {
    take:
    fastq_list
    cram
    bam
    dragen_inputs

    main:
    ch_versions = Channel.empty()

    DRAGEN_FASTQ_LIST(fastq_list, 'fastq_list', dragen_inputs)
    ch_versions = ch_versions.mix(TUMOR_FASTQ_LIST.out.versions)

    DRAGEN_CRAM(cram, 'cram', dragen_inputs)
    ch_versions = ch_versions.mix(TUMOR_CRAM.out.versions)

    DRAGEN_BAM(bam, 'bam', dragen_inputs)
    ch_versions = ch_versions.mix(TUMOR_BAM.out.versions)

    emit:
    ch_versions
}
