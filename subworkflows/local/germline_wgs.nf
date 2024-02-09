include { DRAGEN_GERMLINE_WGS                      } from '../../modules/local/dragen_germline_wgs.nf'
include { DRAGEN_GERMLINE_WGS as DRAGEN_FASTQ      } from '../../modules/local/dragen_germline_wgs.nf'
include { DRAGEN_GERMLINE_WGS as DRAGEN_FASTQ_LIST } from '../../modules/local/dragen_germline_wgs.nf'
include { DRAGEN_GERMLINE_WGS as DRAGEN_CRAM       } from '../../modules/local/dragen_germline_wgs.nf'
include { DRAGEN_GERMLINE_WGS as DRAGEN_BAM       } from '../../modules/local/dragen_germline_wgs.nf'

workflow GERMLINE_WGS {
    take:
    done
    mgi_fastqs
    fastqs
    fastq_list
    cram
    bam

    main:
    ch_versions = Channel.empty()

    DRAGEN_GERMLINE_WGS(mgi_fastqs, 'fastq')
    ch_versions = ch_versions.mix(DRAGEN_GERMLINE_WGS.out.versions)

    DRAGEN_FASTQ(fastqs, 'fastq')
    ch_versions = ch_versions.mix(DRAGEN_FASTQ.out.versions)

    DRAGEN_FASTQ_LIST(fastq_list, 'fastq_list')
    ch_versions = ch_versions.mix(DRAGEN_FASTQ_LIST.out.versions)

    DRAGEN_CRAM(cram, 'cram')
    ch_versions = ch_versions.mix(DRAGEN_CRAM.out.versions)

    DRAGEN_BAM(bam, 'bam')
    ch_versions = ch_versions.mix(DRAGEN_BAM.out.versions)

    emit:
    ch_versions
}