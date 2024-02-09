include { DRAGEN_RNA                        } from '../../modules/local/dragen_rna.nf'
include { DRAGEN_RNA as DRAGEN_FASTQ        } from '../../modules/local/dragen_rna.nf'
include { DRAGEN_RNA as DRAGEN_FASTQ_LIST   } from '../../modules/local/dragen_rna.nf'
include { DRAGEN_RNA as DRAGEN_CRAM         } from '../../modules/local/dragen_rna.nf'
include { DRAGEN_RNA as DRAGEN_BAM         } from '../../modules/local/dragen_rna.nf'


workflow RNA_SEQ {
    take:
    done
    mgi_fastqs
    fastqs
    fastq_list
    cram
    bam

    main:
    ch_versions = Channel.empty()

    DRAGEN_RNA(mgi_fastqs, 'mgi_fastq')
    ch_versions = ch_versions.mix(DRAGEN_RNA.out.versions)

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