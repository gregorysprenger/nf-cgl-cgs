include { SAMPLESHEET_CHECK              } from '../../modules/local/samplesheet_check'
include { SAMPLESHEET_CHECK as MGI_CHECK } from '../../modules/local/samplesheet_check'

workflow INPUT_CHECK {
    take:
    master_samplesheet
    input_dir
    mgi_samplesheet

    main:

    ch_reads_info   = Channel.empty()
    ch_fastq_list   = Channel.empty()
    ch_cram         = Channel.empty()
    ch_bam          = Channel.empty()
    ch_mgi_fastqs   = Channel.empty()


    if (master_samplesheet) {
        SAMPLESHEET_CHECK ( Channel.fromPath(master_samplesheet) )
        .csv
        .splitCsv ( header:true, sep:',' )
        .map { create_master_samplesheet(it) }
        .set { ch_data }

        ch_reads = ch_data.map{meta, read1, read2, fastq_list, cram, bam -> 
            if (read1 == null && read2 != null) {
                throw new RuntimeException("Error: read1 is null, but read2 is not. Check your input data.")
            } else if (read1 != null && read2 == null) {
                throw new RuntimeException("Error: read2 is null, but read1 is not. Check your input data.")
            } else {
                (read1 != null && read2 != null) ? [meta, [read1, read2]] : null
            }
            }
            .filter { it != null }
        // assumes sample_id is library name
        // what do i do if 'N' in index seq
        get_fastq_meta(ch_reads)
        ch_reads_info = get_fastq_meta.out.fastqs.splitCsv ( header:true, sep:',' )
            .map{row ->
            def meta = [:]
            meta.sample_id      = row.sample_id
            meta.library_name   = row.sample_id
            meta.flowcell_id    = row.flowcell_id
            meta.index_sequence = row.index_sequence
            meta.lane_number    = row.lane_number
            [meta, [row.read1, row.read2]]
            // def meta = [:]
            // new_meta.sample_id = row.sample_id
            // new_meta.rgsm = row.sample_id
            // new_meta.rgid = row.flowcell_id + '.' + row.index_sequence + '.' + row.lane_number
            // new_meta.rglb = row.sample_id + '.' + row.index_sequence
            // [new_meta, [row.read1, row.read2]]
            }

        ch_fastq_list = ch_data
            .map{meta, read1, read2, fastq_list, cram, bam -> fastq_list ? [meta, fastq_list] : null}
            .filter { it != null }

        ch_cram = ch_data
            .map{meta, read1, read2, fastq_list, cram, bam -> cram ? [meta, cram] : null}
            .filter { it != null }

        ch_bam = ch_data
            .map{meta, read1, read2, fastq_list, cram, bam -> bam ? [meta, bam] : null}
            .filter { it != null }
    }

    if (mgi_samplesheet && input_dir) {
        MGI_CHECK ( Channel.fromPath(mgi_samplesheet) )
        .csv
        .splitCsv ( header:true, sep:',' )
        .map { create_mgi_samplesheet(it) }
        .set { ch_mgi }

        ch_fastqs = Channel.fromFilePairs("${input_dir}/*_R{1,2}*")

        ch_samplesheet_meta = ch_mgi
            .map { meta ->
            name = meta.read1.replaceAll(~/_(R[12]).*/, '')
            [name,meta]
        }

        ch_mgi_fastqs = ch_samplesheet_meta.join(ch_fastqs).map{ name, meta, fastqs -> 
            [meta, fastqs]
        }
    }
    done = ch_reads_info.mix(ch_fastq_list,ch_cram,ch_bam,ch_mgi_fastqs).collect()

    emit:
    done
    ch_reads_info
    ch_fastq_list
    ch_cram
    ch_bam
    ch_mgi_fastqs
}

def create_master_samplesheet(LinkedHashMap row) {
    def meta = [:]
    meta.sample_id    = row.sample_id

    def sample_data = []
    def read1 = null
    if (row.read1) {
        if (!file(row.read1).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Read 1 file does not exist!\n${row.read1}"
        }
        read1 = file(row.read1)
    }
    def read2 = null
    if (row.read2) {
        if (!file(row.read2).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Read 2 file does not exist!\n${row.read2}"
        }
        read2 = file(row.read2)
    }
    def fastq_list = null
    if (row.fastq_list) {
        if (!file(row.fastq_list).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Fastqlist file does not exist!\n${row.fastq_list}"
        }
        fastq_list = file(row.fastq_list)
    }
    def cram = null
    if (row.cram) {
        if (!file(row.cram).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Cram file does not exist!\n${row.cram}"
        }
        cram = file(row.cram)
    }
    def bam = null
    if (row.bam) {
        if (!file(row.bam).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Bam file does not exist!\n${row.bam}"
        }
        bam = file(row.bam)
    }
    sample_data = [meta, read1, read2, fastq_list, cram, bam]
    return sample_data
}

def create_mgi_samplesheet(LinkedHashMap row) {
    def meta = [:]
    meta.sample_id      = row.'Library Name'        ?: null
    meta.read1          = row.'FASTQ Path - Read 1' ?: null
    meta.read2          = row.'FASTQ Path - Read 2' ?: null
    meta.flowcell_id    = row.'Flowcell ID'         ?: null
    meta.index_sequence = row.'Index Sequence'      ?: null

    def indexSequence = row.'Index Sequence'        ?: null
    if (indexSequence && indexSequence.contains('-')) {
        def (i7, i5) = indexSequence.split('-', 2)
        meta.i7index = i7
        meta.i5index = i5
    }

    meta.lane_number        = row.'Flowcell Lane'                 ?: null
    meta.espid              = row.'ESP ID'                        ?: null
    meta.library_name       = row.'Library Name'                  ?: null
    meta.species            = row.'Species'                       ?: null
    meta.illuminaSampleType = row.'Illumina Sample Type'          ?: null
    meta.libraryType        = row.'Library Type'                  ?: null
    meta.libraryName        = row.'Library Name'                  ?: null
    meta.dateComplete       = row.'Date Complete'                 ?: null
    meta.totalReads         = row.'Total Reads'                   ?: null
    meta.totalBases         = row.'Total Bases'                   ?: null
    meta.avgQScoreRead1     = row.'Avg Q Score Read 1'            ?: null
    meta.avgQScoreRead2     = row.'Avg Q Score Read 2'            ?: null
    meta.percentQ30Read1    = row.'% >Q30 Read 1'                 ?: null
    meta.percentQ30Read2    = row.'% >Q30 Read 2'                 ?: null
    meta.errorread1         = row.'PhiX Error Rate Read 1'        ?: null
    meta.errorread2         = row.'PhiX Error Rate Read 2'        ?: null
    meta.passfilread1       = row.'% Pass Filter Clusters Read 1' ?: null
    meta.passfilread2       = row.'% Pass Filter Clusters Read 2' ?: null

    return meta
}

process get_fastq_meta {
    label 'process_single'
    container "quay.io/biocontainers/python:3.8.3"

    input:
    tuple val(meta), path(fastqs)

    output:
    path("*.csv"), emit: fastqs

    script:
    """
    get_fastq_info.py $meta.sample_id *_R1* *_R2*
    """

}