include { DRAGEN_TUMOR_NORMAL                      } from '../../modules/local/dragen_tumor_normal.nf'
include { DRAGEN_TUMOR_NORMAL as TUMOR_FASTQ       } from '../../modules/local/dragen_tumor_normal.nf'
include { DRAGEN_TUMOR_NORMAL as TUMOR_FASTQ_LIST  } from '../../modules/local/dragen_tumor_normal.nf'
include { DRAGEN_TUMOR_NORMAL as TUMOR_CRAM        } from '../../modules/local/dragen_tumor_normal.nf'
include { DRAGEN_TUMOR_NORMAL as TUMOR_BAM         } from '../../modules/local/dragen_tumor_normal.nf'

workflow TUMOR_NORMAL {
    take:
    fastq_list
    cram
    bam
    dragen_inputs
    
    main:
    ch_versions = Channel.empty()

    DRAGEN_TUMOR_NORMAL(mgi_fastqs, 'fastq')
    ch_versions = ch_versions.mix(DRAGEN_TUMOR_NORMAL.out.versions)

    normal_fastqs = fastqs.map{meta, fastqs ->
        def prefix = meta."sample_id".split("-")[0]
        [prefix, meta, fastqs]
        }
        .filter { it[1]['sample_id'].contains("normal") }

    tumor_fastqs = fastqs.map { meta, fastqs ->
        def prefix = meta."sample_id".split("-")[0]
        [prefix, meta, fastqs]
        }
        .filter { !it[1]['sample_id'].contains("normal") }

    fastq_input = normal_fastqs
    .combine(tumor_fastqs, by: 0)
    .map{prefix, normal_meta, normal_fastqs, tumor_meta, tumor_fastqs -> [normal_meta, normal_fastqs, tumor_meta, tumor_fastqs]}
    
    TUMOR_FASTQ(fastq_input, 'fastq')
    ch_versions = ch_versions.mix(TUMOR_FASTQ.out.versions)

    TUMOR_FASTQ_LIST(fastq_list, 'fastq_list')
    ch_versions = ch_versions.mix(TUMOR_FASTQ_LIST.out.versions)

    TUMOR_CRAM(cram, 'cram')
    ch_versions = ch_versions.mix(TUMOR_CRAM.out.versions)

    TUMOR_BAM(bam, 'bam')
    ch_versions = ch_versions.mix(TUMOR_BAM.out.versions)

    emit:
    ch_versions
}
