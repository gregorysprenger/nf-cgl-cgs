/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CONVERT_XLSX_TO_CSV } from '../../modules/local/convert_xlsx_to_csv'

/*
========================================================================================
    SUBWORKFLOW TO CHECK INPUTS
========================================================================================
*/

workflow INPUT_CHECK {

    take:
    mgi_samplesheet  //  string: Path to input MGI samplesheet
    ch_fastq_list    //  channel: [ path(file) ]

    main:
    ch_versions        = Channel.empty()
    ch_samples         = Channel.empty()
    ch_mgi_samplesheet = Channel.empty()

    /*
    ================================================================================
                        Check if batch name is part of Illumina run directory
    ================================================================================
    */

    if (params.batch_name && params.illumina_rundir && !params.validation_samples) {
        def dateMatch = params.batch_name.find(/(\d{8})/)
        params.illumina_rundir.split('/')[0].split('_').any{ part -> part == dateMatch }
    } else {
        error("Date in batch name does not match Illumina run directory! If this is a validation run, please set the '--validation_samples' parameter to true.")
    }

    /*
    ================================================================================
                        Process input MGI samplesheet
    ================================================================================
    */

    if (mgi_samplesheet) {
        samplesheet = Channel.fromPath(mgi_samplesheet.split(',') as List, checkIfExists: true)

        // Verify MGI samplesheet has a file extension in [xlsx,csv,tsv]
        if (samplesheet.map{ hasExtension(it, 'xlsx') }) {
            CONVERT_XLSX_TO_CSV (
                samplesheet
            )
            ch_versions        = ch_versions.mix(CONVERT_XLSX_TO_CSV.out.versions)
            ch_mgi_samplesheet = ch_mgi_samplesheet.mix(CONVERT_XLSX_TO_CSV.out.csv)

        } else if (samplesheet.any{ hasExtension(it, 'csv') || hasExtension(it, 'tsv') }) {
            ch_mgi_samplesheet = ch_mgi_samplesheet.mix(samplesheet)
        } else {
            error("MGI samplesheet input does not end in '.{xlsx,csv,tsv}'!")
        }
    }

    /*
    ================================================================================
                        Process input FastQ list
    ================================================================================
    */

    ch_samples = ch_samples.mix(
                    ch_fastq_list
                        .filter{ it != [] }
                        .flatMap{
                            def data = parseFastqList(it)
                            def requiredColumns = ['RGID', 'RGSM', 'RGLB', 'Lane', 'Read1File', 'Read2File']
                            data.collect{
                                if (!it.keySet().containsAll(requiredColumns)) {
                                    error("Missing required columns in input FastQ list!")
                                }

                                def R1 = file(it['Read1File'], checkIfExists: true)
                                def R2 = file(it['Read2File'], checkIfExists: true)

                                def regexPattern = /\w\d{2}-\d+/
                                def meta = [
                                    id  : it.RGSM,
                                    acc : (it.RGSM =~ regexPattern)?.find() ?: it.RGSM,
                                    RGSM: it.RGSM
                                ]

                                [ meta.acc, meta, [ R1, R2 ] ]
                            }
                        }
                        .groupTuple()
                        .combine(
                            ch_fastq_list
                                .filter{ it != [] }
                                .map{
                                    def data = parseFastqList(it)
                                    data.each{
                                        if (it) {
                                            it['Read1File'] = "fastq_files/${it['Read1File'].split('/')[-1]}"
                                            it['Read2File'] = "fastq_files/${it['Read2File'].split('/')[-1]}"
                                        }
                                    }

                                    if (data) {
                                        def header = data[0].keySet().join(',')
                                        def content = data.collect { it.values().join(',') }.join('\n')

                                        [ [ header ], [ content ] ]
                                    } else {
                                        [ [], [] ]
                                    }
                                }
                                .flatten()
                                .collectFile(
                                    name   : "updated_fastq_list.csv",
                                    newLine: true,
                                    sort   : 'index'
                                )
                        )
                        .map{ id, meta, reads, fastq_list -> [ meta[0], reads.flatten(), fastq_list ] }
                )

    emit:
    samples         = ch_samples          // channel: [ val(sample_info), path(reads), path(fastq_list) ]
    mgi_samplesheet = ch_mgi_samplesheet  // channel: [ path(file) ]
    versions        = ch_versions         // channel: [ path(file) ]

}

/*
========================================================================================
    FUNCTIONS
========================================================================================
*/

// Get file extension
def hasExtension(it, extension) {
    it.toString().toLowerCase().endsWith(extension.toLowerCase())
}

// Parse FastQ list
def parseFastqList(file) {
    def separator = file.endsWith("tsv") ? '\t' : ','
    def lines = file.readLines()
    def headers = lines.first().split(separator)
    lines.drop(1).collect{ line ->
        [headers, line.split(separator)].transpose().collectEntries{ it }
    }
}
