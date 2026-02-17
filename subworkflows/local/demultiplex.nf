/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CREATE_DEMULTIPLEX_SAMPLESHEET   } from '../../modules/local/create_demultiplex_samplesheet'
include { DRAGEN_DEMULTIPLEX               } from '../../modules/local/dragen_demultiplex'
include { INPUT_CHECK as VERIFY_FASTQ_LIST } from '../../subworkflows/local/input_check'


// Validate illumina run completion status
def validate_run = { f ->
    def matcher = f.text =~ /<RunStatus>\s*(.*?)\s*<\/RunStatus>/
    if (matcher && matcher[0][1] == 'RunCompleted') {
        log.info "[DEMULTIPLEX] Run status 'RunCompleted' confirmed for ${f} – continuing."
        return f.parent
    }
    def runStatusInfo
    if (!matcher) {
        runStatusInfo = "RunStatus tag not found"
    } else if (matcher[0].size() > 1 && matcher[0][1]) {
        runStatusInfo = "found status '${matcher[0][1]}'"
    } else {
        runStatusInfo = "RunStatus tag empty or malformed"
    }
    error("${f} exists but did not complete successfully: ${runStatusInfo} (expected 'RunCompleted').")
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CREATE CHANNELS FOR INPUT PARAMETERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

/*
========================================================================================
    SUBWORKFLOW TO DEMULTIPLEX DATA
========================================================================================
*/

workflow DEMULTIPLEX {

    take:
    ch_samplesheet  // channel: [ path(file) ]

    main:
    ch_illumina_run_dir = Channel.empty()
    ch_versions         = Channel.empty()

    // Verify presence of Illumina run directory if there are samples to demultiplex
    ch_samplesheet.map{
        !it.isEmpty() && !params.illumina_rundir
            ? error("Please specify the path to the directory containing the Illumina run data.")
            : it
    }

    // Watch for RunCompletionStatus.xml files in each specified Illumina run directory
    if (params.illumina_rundir) {
        for (dirRaw in params.illumina_rundir.toString().split(',')) {
            def dir = dirRaw.trim()
            if (!dir) {
                continue
            }
            def xml = file("${dir}/RunCompletionStatus.xml")
            log.info "[${new java.util.Date().format('yyyy-MM-dd HH:mm:ss')}] [DEMULTIPLEX] Waiting for ${xml} to be created …"

            def chNew
            if (xml.exists()) {
                log.info "[${new java.util.Date().format('yyyy-MM-dd HH:mm:ss')}] [DEMULTIPLEX] ${xml} file exists – continuing."
                chNew = Channel.fromPath(xml.toString())
            } else {
                chNew = Channel.watchPath(xml.toString())
                            .take(1)
                            .map{
                                log.info "[${new java.util.Date().format('yyyy-MM-dd HH:mm:ss')}] [DEMULTIPLEX] ${xml} appeared – continuing."
                                it
                            }
            }
            ch_illumina_run_dir = ch_illumina_run_dir.mix(chNew.map(validate_run))
        }
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
                            ch_illumina_run_dir.map{ [ it.name.toString().split('_').last().takeRight(9), it ] },
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
            .map{
                flowcell, samplesheet, illumina_run_dir ->
                    def run_dir_count = params.illumina_rundir ? params.illumina_rundir.toString().split(',').collect{ it.trim() }.findAll{ it }.size() : 0
                    def meta = ['flowcell': run_dir_count > 1 ? flowcell : '']
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

    // Use 'params.demux_outdir' path for paths in 'fastq_list.csv' and save
    if (params.demux_outdir) {
        def batch_name = params.batch_name ?: new java.util.Date().format('yyyyMMdd') + '_CGS'

        ch_fastq_list = VERIFY_FASTQ_LIST.out.samples
            .map{ meta, reads, fastq_list -> fastq_list }
            .splitCsv( header: true )
            .map{
                row ->
                    def read1_parts = row['Read1File'].split('/')
                    def read1 = read1_parts.size() > 1 ? read1_parts[-2..-1].join('/') : read1_parts[-1]

                    def read2_parts = row['Read2File'].split('/')
                    def read2 = read2_parts.size() > 1 ? read2_parts[-2..-1].join('/') : read2_parts[-1]

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
    samples  = VERIFY_FASTQ_LIST.out.samples  // channel: [ val(meta), path(file) ]
    versions = ch_versions                    // channel: [ path(file) ]

}
