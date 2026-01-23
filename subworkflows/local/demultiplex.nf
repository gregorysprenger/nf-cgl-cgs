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
    ? Channel.fromPath(params.illumina_rundir.split(',') as List, type: 'dir', checkIfExists: true)
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

    // Flowcell specific demultiplex channel: [ val(flowcell), path(samplesheet), path(illumina run dir) ]
    ch_demux_data = ch_samplesheet
                        .map{
                            !it.isEmpty() && !params.illumina_rundir
                                ? error("Please specify the path to the directory containing the Illumina run data.")
                                : it
                        }
                        .splitCsv(header: true, quote: '"')
                        .map{ [ it['Flowcell ID'].split('_').last().takeRight(9), it ] }
                        .groupTuple(by: 0)
                        .map{
                            flowcell, rows ->
                                def columns = rows[0].keySet() as List
                                def samplesheet = file("${workDir}/Samplesheet_${flowcell}.csv")
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
                            ch_illumina_run_dir.map{ dir -> [ dir.name.toString().split('_').last().takeRight(9), dir ] },
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
        CREATE_DEMULTIPLEX_SAMPLESHEET.out.samplesheet
            .map{ flowcell, samplesheet -> [ flowcell, samplesheet ] }
            .join(
                ch_illumina_run_dir.map{ dir -> [ dir.name.toString().split('_').last().takeRight(9), dir ] },
                by: 0
            )
            .combine(CREATE_DEMULTIPLEX_SAMPLESHEET.out.samplesheet.count())
            .map{
                flowcell, samplesheet, illumina_run_dir, num_samples ->
                    def meta = ['flowcell': num_samples > 1 ? flowcell : null ]
                    [ meta, samplesheet, illumina_run_dir ]
            }
    )
    ch_versions = ch_versions.mix(DRAGEN_DEMULTIPLEX.out.versions)

    //
    // SUBWORKFLOW: Verify fastq_list.csv
    //
    VERIFY_FASTQ_LIST (
        [],
        DRAGEN_DEMULTIPLEX.out.fastq_list
    )
    ch_versions = ch_versions.mix(VERIFY_FASTQ_LIST.out.versions)

    emit:
    samples  = VERIFY_FASTQ_LIST.out.samples  // channel: [ val(meta), path(file) ]
    versions = ch_versions                    // channel: [ path(file) ]

}
