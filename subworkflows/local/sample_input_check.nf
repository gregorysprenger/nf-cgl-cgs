include { SAMPLESHEET_CHECK              } from '../../modules/local/samplesheet_check.nf'
include { MAKE_FASTQLIST                 } from '../../modules/local/make_fastqlist.nf'

/*
Subworkflow that checks and reformats input data (via python script) that is passed
as either read1/read2 pairs, an illumina fastq_list.csv file, or cram files or bam files.
Channel operations are used to create an output channel that has meta, a value of 'fastq', 'cram', or 'bam'
so that the next workflow can determine how to process the data, and then a list of files.
*/

workflow SAMPLE_INPUT_CHECK {
    take:
    master_samplesheet
    data_path

    main:

    ch_mastersheet        = Channel.empty()
    ch_input_data         = Channel.empty()
    ch_dragen_outputs     = Channel.empty()
    ch_versions           = Channel.empty()

    // Runs a python script that parses the sample sheet and adds key metadata, 
    // including index sequences, flowcell, and lane. If fastq_list.csv files are passed,
    // these are parsed also and read1/read2 pairs are returned. The output is then channelified.
    SAMPLESHEET_CHECK ( master_samplesheet, data_path )
    .csv
    .splitCsv ( header:true, sep:',' )
    .map { create_master_samplesheet(it) }
    .set { ch_mastersheet }

    ch_versions = ch_versions.mix(SAMPLE_INPUT_CHECK.out.versions)

    // Organize reads into a fastq list string (to be written to a fastq_list file) and read1/read2 pairs.
    ch_mastersheet
    .map { meta -> 
        if (meta.read1 != null && meta.read2 != null){
            def new_meta = meta.subMap('id', 'mrn', 'accession','dob','sex','family_id','relationship')
            def rgid = meta.flowcell + '.' + meta.i7index + '.' + meta.i5index + '.' + meta.lane 
            def rglb = meta.id + '.' + meta.i7index + '.' + meta.i5index
            [ new_meta, [ rgid, meta.id, rglb, meta.lane, file(meta.read1), file(meta.read2) ] ]
        }
    }
    .groupTuple()
    .map { meta, fqlist -> 
            def fileList = ['RGID,RGSM,RGLB,Lane,Read1File,Read2File']
            def read1 = []
            def read2 = []

            // Create data rows
            for (int i = 0; i < fqlist.size(); i++) {
                def row = fqlist[i]
                read1 << file(row[4])
                read2 << file(row[5])
                fileList << [ row[0], row[1], row[2], row[3], row[4].toString().split('/')[-1], row[5].toString().split('/')[-1] ].join(',')
            }
            return [ meta, fileList.join('\n'), read1, read2 ]
    }
    .set { ch_fastqs }

    // Put read1 and read2 files into separate channels.
    ch_fastqs
    .map { meta, fqlist, read1, read2 -> [ meta, read1 ] }
    .transpose()
    .set { ch_read1 }

    ch_fastqs
    .map { meta, fqlist, read1, read2 -> [ meta, read2 ] }
    .transpose()
    .set { ch_read2 }

    // Make fastq_list file from string. if I could figure out how to do this with a channel operation, I would.
    ch_fastqs
    .map { meta, fqlist, read1, read2 -> [ meta, fqlist ] } | MAKE_FASTQLIST

    // Concatenate read1, read2, and fastq_list files group by meta.
    MAKE_FASTQLIST.out.fastq_list
    .concat(ch_read1,ch_read2)
    .transpose()
    .groupTuple()
    .map { meta, files -> 
        return [ meta, 'fastq', files ]
    }
    .set { ch_input_data }

    // Organize cram files into a channel.
    ch_mastersheet
    .map { meta -> 
        if (meta.cram != null){
            def new_meta = meta.subMap('id', 'mrn', 'accession','dob','sex','family_id','relationship')
            return [ new_meta, 'cram', [ file(meta.cram), file(meta.cram + '.crai') ] ]
        }
        if (meta.bam != null){
            def new_meta = meta.subMap('id', 'mrn', 'accession','dob','sex','family_id','relationship')
            return [ new_meta, 'bam', [ file(meta.bam), file(meta.bam + '.crai') ] ]
        }
    }
    .set { ch_aligned }

    ch_input_data = ch_input_data.mix(ch_aligned)
    
    ch_mastersheet
    .map { meta -> 
        if (meta.dragen_path != null){
            def new_meta = meta.subMap('id', 'mrn', 'accession','dob','sex','family_id','relationship')
            return [ new_meta, file(meta.dragen_path).listFiles() ]
        }
    }
    .set { ch_dragen_outputs }

    emit:
    versions = ch_versions
    dragen_outputs = ch_dragen_outputs
    input_data = ch_input_data
}

def create_master_samplesheet(LinkedHashMap row) {

    def meta = [:]
    meta.id             = row.id
    meta.mrn            = row.mrn ?: null
    meta.accession      = row.accession ?: null
    meta.dob            = row.dob ?: null
    meta.sex            = row.sex ?: null
    meta.family_id      = row.family_id ?: null
    meta.relationship   = row.relationship ?: null
    meta.i7index        = row.i7index ?: null
    meta.i5index        = row.i5index ?: null
    meta.flowcell       = row.flowcell ?: null
    meta.lane           = row.lane ?: null

    if (row.read1) {
        if (!file(row.read1).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Read 1 file does not exist!\n${row.read1}"
        }
        meta.read1 = file(row.read1)
    }
    if (row.read2) {
        if (!file(row.read2).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Read 2 file does not exist!\n${row.read2}"
        }
        meta.read2 = file(row.read2)
    }

    if (row.cram) {
        if (!file(row.cram).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Cram file does not exist!\n${row.cram}"
        }
        meta.cram = file(row.cram)
    }

    if (row.bam) {
        if (!file(row.bam).exists()) {
        exit 1, "ERROR: Please check input samplesheet -> Bam file does not exist!\n${row.bam}"
        }
        meta.bam = file(row.bam)
    }

    if (row.dragen_path) {
        if (!file(row.dragen_path).exists()) {
            exit 1, "ERROR: Please check input samplesheet -> dragen_path does not exist!\n${row.dragen_path}"
        }
        meta.dragen_path = file(row.dragen_path)
    }

    return meta
}
