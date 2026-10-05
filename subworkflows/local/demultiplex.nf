/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CREATE_DEMULTIPLEX_SAMPLESHEET   } from '../../modules/local/create_demultiplex_samplesheet'
include { CREATE_DEMUX_FASTQ_LIST          } from '../../modules/local/create_demux_fastq_list'
include { DRAGEN_DEMULTIPLEX               } from '../../modules/local/dragen_demultiplex'
include { INPUT_CHECK as VERIFY_FASTQ_LIST } from '../../subworkflows/local/input_check'


// Validate illumina run completion status
import groovy.xml.XmlSlurper

def validate_run = { f ->
    def xml
    try {
        xml = new XmlSlurper().parse(f.toFile())
    } catch (Exception e) {
        error("${f} exists but could not be parsed as XML: ${e.message}")
    }

    // Find all RunStatus elements anywhere in the document, regardless of namespace
    def runStatusNodes = xml.'**'.findAll{ it.name() == 'RunStatus' }
    def runStatuses = runStatusNodes
        .collect{ it.text()?.trim() }
        .findAll{ it }

    if (runStatuses.any{ it == 'RunCompleted' }) {
        log.info "[DEMULTIPLEX] Run status 'RunCompleted' confirmed for ${f} – continuing."
        return f.parent
    }

    def runStatusInfo
    if (!runStatusNodes || runStatusNodes.isEmpty()) {
        runStatusInfo = "RunStatus tag not found"
    } else if (runStatuses && !runStatuses.isEmpty()) {
        if (runStatuses.size() == 1) {
            runStatusInfo = "found status '${runStatuses[0]}'"
        } else {
            runStatusInfo = "found statuses '${runStatuses.join("', '")}'"
        }
    } else {
        runStatusInfo = "RunStatus tag empty or malformed"
    }
    error("${f} exists but did not complete successfully: ${runStatusInfo} (expected 'RunCompleted').")
}

/*
========================================================================================
    SUBWORKFLOW TO DEMULTIPLEX DATA
========================================================================================
*/

workflow DEMULTIPLEX {

    take:
    ch_samplesheet  // channel: [ path(file) ]
    ch_fastq_list   // channel: [ path(file) ]

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
            if (!file(dir).isDirectory()) {
                error("Illumina run directory not found: ${dir}")
            }
            def xml = file("${dir}/RunCompletionStatus.xml")
            log.info "[DEMULTIPLEX] Checking for ${xml} (will wait if missing) …"

            def chNew
            if (xml.exists()) {
                log.info "[DEMULTIPLEX] ${xml} file exists – continuing."
                chNew = Channel.fromPath(xml.toString())
            } else {
                chNew = Channel.watchPath(xml.toString())
                            .take(1)
                            .map{
                                log.info "[DEMULTIPLEX] ${xml} appeared – continuing."
                                it
                            }
            }
            ch_illumina_run_dir = ch_illumina_run_dir.mix(chNew.map(validate_run))
        }
    }

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
                            ch_illumina_run_dir.filter{ it != null }.map{ [ it.name.toString().split('_').last().takeRight(9), it ] },
                            by: 0,
                            remainder: true
                        )
                        // An unmatched run directory would otherwise be dropped silently, letting alignment start without demultiplexing
                        .filter{
                            flowcell, samplesheet, illumina_run_dir ->
                                if (!samplesheet) {
                                    error("Illumina run directory '${illumina_run_dir}' (flowcell '${flowcell}') does not match any 'Flowcell ID' in the input samplesheet.")
                                }
                                if (!illumina_run_dir) {
                                    log.warn("Flowcell '${flowcell}' in the input samplesheet does not match any '--illumina_rundir' and will not be demultiplexed.")
                                }
                                illumina_run_dir
                        }

    //
    // MODULE: Create demultiplex samplesheet
    //
    CREATE_DEMULTIPLEX_SAMPLESHEET (
        ch_demux_data
    )
    ch_versions = ch_versions.mix(CREATE_DEMULTIPLEX_SAMPLESHEET.out.versions)

    // Determine how many distinct flowcells will be demultiplexed, based on the 'samplesheet' channel output
    def ch_demux_flowcell_count = CREATE_DEMULTIPLEX_SAMPLESHEET.out.samplesheet
                                    .map{ it[0] }
                                    .unique()
                                    .count()

    //
    // MODULE: Demultiplex samples
    //
    DRAGEN_DEMULTIPLEX (
        CREATE_DEMULTIPLEX_SAMPLESHEET.out.samplesheet
            .join(ch_demux_data.map{ flowcell, samplesheet, rundir -> [ flowcell, rundir ] }, by: 0)
            .combine(ch_demux_flowcell_count)
            .map{ flowcell, samplesheet, illumina_run_dir, flowcell_count ->
                def meta = [ 'flowcell': flowcell_count > 1 ? flowcell : '' ]
                [ meta, samplesheet, illumina_run_dir ]
            }
    )
    ch_versions = ch_versions.mix(DRAGEN_DEMULTIPLEX.out.versions)

    //
    // SUBWORKFLOW: Verify fastq_list.csv, grouping FastQ files from all flowcells and the input FastQ list by accession
    //
    VERIFY_FASTQ_LIST (
        [],
        DRAGEN_DEMULTIPLEX.out.fastq_list.map{ meta, fastq_list -> fastq_list }.mix(ch_fastq_list),
        Channel.empty()
    )
    ch_versions = ch_versions.mix(VERIFY_FASTQ_LIST.out.versions)

    //
    // MODULE: Create 'fastq_list.csv' report for each flowcell, keeping the FastQ paths in the work directory
    //
    if (params.demux_outdir) {
        CREATE_DEMUX_FASTQ_LIST (
            DRAGEN_DEMULTIPLEX.out.fastq_list
        )
        ch_versions = ch_versions.mix(CREATE_DEMUX_FASTQ_LIST.out.versions)
    }

    emit:
    samples  = VERIFY_FASTQ_LIST.out.samples  // channel: [ val(meta), path(reads), path(fastq_list), path(alignment_file) ]
    versions = ch_versions                    // channel: [ path(file) ]

}
