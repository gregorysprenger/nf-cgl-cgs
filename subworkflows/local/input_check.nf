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
    mgi_samplesheet //  string: Path to input MGI samplesheet
    ch_fastq_list   //  string: Path to input fastq_list.csv

    main:
    ch_versions = Channel.empty()
    ch_mgi_samplesheet = Channel.empty()

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

    if (ch_fastq_list) {
        // Set separator for input FastQ list
        fastq_list_separator =  ch_fastq_list.map{ hasExtension(it, 'csv') } ? ','  :
                                ch_fastq_list.map{ hasExtension(it, 'tsv') } ? '\t' :
                                error("Input for `--fastq_list` does not end in `.{csv,tsv}`!")

        // Parse FastQ list and verify columns
        ch_samples = ch_fastq_list
                        .splitCsv( header: true, sep: fastq_list_separator )
                        .map{
                            row ->
                                if (row.size() < 6) {
                                    error("Input for `--fastq_list` requires at least 6 columns but received ${row.size()}.")
                                }

                                def requiredColumns = ['RGID', 'RGSM', 'RGLB', 'Lane', 'Read1File', 'Read2File']
                                def missingColumn = requiredColumns.find{ !row[it] }
                                if (missingColumn) {
                                    error("Missing '${missingColumn}' column in input FastQ list!")
                                }

                                def R1 = row.Read1File ? file(row.Read1File, checkIfExists: true) : error("Missing or invalid 'Read1File' file!")
                                def R2 = row.Read2File ? file(row.Read2File, checkIfExists: true) : error("Missing or invalid 'Read2File' file!")

                                def regexPattern = /\w\d{2}-\d+/
                                def meta = [
                                    id  : row.RGSM,
                                    acc : row.RGSM.findAll(regexPattern) ?: row.RGSM,
                                    RGID: row.RGID,
                                    RGSM: row.RGSM
                                ]
                                [ meta, R1, R2 ]
                        }

    } else {
        ch_samples = Channel.empty()
    }

    emit:
    samples         = ch_samples         // channel: [ val(sample_info), path(read1), path(read2) ]
    mgi_samplesheet = ch_mgi_samplesheet // channel: [ path(file) ]
    versions        = ch_versions        // channel: [ path(file) ]

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
