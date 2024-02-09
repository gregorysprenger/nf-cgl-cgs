include { DRAGEN_5MC                      } from '../../modules/local/dragen_5mc.nf'
include { DRAGEN_5MC as DRAGEN_FASTQS     } from '../../modules/local/dragen_5mc.nf'
include { DRAGEN_5MC as DRAGEN_FASTQ_LIST } from '../../modules/local/dragen_5mc.nf'
include { DRAGEN_5MC as DRAGEN_CRAM       } from '../../modules/local/dragen_5mc.nf'
include { DRAGEN_5MC as DRAGEN_BAM       } from '../../modules/local/dragen_5mc.nf'

workflow METHYLATION {
    take:
    done
    mgi_fastqs
    fastqs
    fastq_list
    cram
    bam

    main:
    ch_versions = Channel.empty()

    DRAGEN_5MC(mgi_fastqs, 'mgi_fastq')
    ch_versions = ch_versions.mix(DRAGEN_5MC.out.versions)

    DRAGEN_FASTQS(fastqs, 'fastq')
    ch_versions = ch_versions.mix(DRAGEN_FASTQS.out.versions)

    DRAGEN_FASTQ_LIST(fastq_list, 'fastq_list')
    ch_versions = ch_versions.mix(DRAGEN_FASTQ_LIST.out.versions)

    DRAGEN_CRAM(cram, 'cram')
    ch_versions = ch_versions.mix(DRAGEN_CRAM.out.versions)

    DRAGEN_BAM(bam, 'bam')
    ch_versions = ch_versions.mix(DRAGEN_BAM.out.versions)

    emit: 
    ch_versions

}