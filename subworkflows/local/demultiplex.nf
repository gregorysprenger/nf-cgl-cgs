/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CREATE_DEMULTIPLEX_SAMPLESHEET   } from '../../modules/local/create_demultiplex_samplesheet'
include { DRAGEN_DEMULTIPLEX               } from '../../modules/local/dragen_demultiplex'
include { INPUT_CHECK as VERIFY_FASTQ_LIST } from '../../subworkflows/local/input_check'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CREATE CHANNELS FOR INPUT PARAMETERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Illumina run directory
ch_illumina_run_dir = params.illumina_rundir
    ? Channel.fromPath(params.illumina_rundir.split(',') as List, type: 'dir', checkIfExists: true).collect()
    : Channel.empty()

/*
========================================================================================
    SUBWORKFLOW TO DEMULTIPLEX DATA
========================================================================================
*/

workflow DEMULTIPLEX {

    take:
    ch_samplesheet  // channel: [ path(file) ]

    main:
    ch_versions = Channel.empty()

    // Verify presence of Illumina run directory if there are samples to demultiplex
    ch_samplesheet.map{
        !it.isEmpty() && !params.illumina_rundir
            ? error("Please specify the path to the directory containing the Illumina run data.")
            : it
    }

    // Flowcell specific demultiplex channel: [ val(flowcell), path(samplesheet), path(illumina run dir) ]
    ch_demux_data = ch_samplesheet
                        .splitCsv(header: true, quote: '"')
                        .map{ [ it['Flowcell ID'].split('_').last().takeRight(9), it ] }
                        .groupTuple(by: 0)
                        .map{
                            flowcell, rows ->
                                def columns = rows[0].keySet() as List
                                def samplesheet = file("Samplesheet_${flowcell}.csv")
                                samplesheet.text = columns.join(',') + '\n' +
                                    rows.collect{ r ->
                                        columns.collect{ c ->
                                            def value = r[c]
                                            c == 'Lane' ? "\"${value}\"" : value
                                        }.join(',')
                                    }.join('\n')
                                [ flowcell, samplesheet ]
                        }
                        .join(
                            ch_illumina_run_dir.map{ [ it[0].name.toString().split('_').last().takeRight(9), it[0] ] },
                            by: 0
                        )

    //
    // MODULE: Create demultiplex samplesheet
    //
    CREATE_DEMULTIPLEX_SAMPLESHEET (
        ch_demux_data
    )
    ch_versions = ch_versions.mix(CREATE_DEMULTIPLEX_SAMPLESHEET.out.versions)

    //
    // MODULE: Demultiplex samples
    //
    DRAGEN_DEMULTIPLEX (
        CREATE_DEMULTIPLEX_SAMPLESHEET.out.demux_data
            .count()
            .combine(CREATE_DEMULTIPLEX_SAMPLESHEET.out.demux_data)
            .map{
                count, flowcell, samplesheet, illumina_run_dir ->
                    def meta = ['flowcell': count > 1 ? flowcell : '']
                    [ meta, samplesheet, illumina_run_dir ]
            }
    )
    ch_versions = ch_versions.mix(DRAGEN_DEMULTIPLEX.out.versions)

    //
    // SUBWORKFLOW: Verify fastq_list.csv
    //
    VERIFY_FASTQ_LIST (
        [],
        DRAGEN_DEMULTIPLEX.out.fastq_list,
        Channel.empty()
    )
    ch_versions = ch_versions.mix(VERIFY_FASTQ_LIST.out.versions)

    // Use 'params.demux_outdir' path for paths in 'fastq_list.csv' and save
    if (params.demux_outdir) {
        def batch_name = params.batch_name ?: new java.util.Date().format('yyyyMMdd') + '_CGS'

        ch_fastq_list = VERIFY_FASTQ_LIST.out.samples
            .map{ meta, reads, fastq_list, alignment_file -> fastq_list }
            .splitCsv( header: true )
            .map{
                row ->
                    def read1 = row['Read1File'].split('/')[-2..-1].join('/')
                    def read2 = row['Read2File'].split('/')[-2..-1].join('/')

                    // Get absolute path of 'params.demux_outdir'
                    def demux_outdir = file(params.demux_outdir).toAbsolutePath().toString()

                    row['Read1File'] = "${demux_outdir}/${read1}"
                    row['Read2File'] = "${demux_outdir}/${read2}"

                    return "${row.keySet().join(',')}\n${row.values().join(',')}\n"
            }
            .collectFile(
                name      : "fastq_list.csv",
                keepHeader: true,
                storeDir  : "${params.demux_outdir}/${batch_name}/Reports/"
            )
    }

    emit:
    samples  = VERIFY_FASTQ_LIST.out.samples  // channel: [ val(meta), path(reads), path(fastq_list), path(alignment_file) ]
    versions = ch_versions                    // channel: [ path(file) ]

}
