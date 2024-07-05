/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

/*
========================================================================================
    SUBWORKFLOW TO CHECK INPUTS
========================================================================================
*/

workflow INPUT_CHECK {

    take:
    input

    main:

    // Set separator for input FastQ list
    if (hasExtension(input, 'csv')) {
        input_separator=','
    } else if (hasExtension(input, 'tsv')) {
        input_separator='\t'
    } else {
        error("ERROR: Input file does not end in `.csv` or `.tsv`!")
    }

    // Parse FastQ list and verify columns
    ch_parse_input = Channel.fromPath(input, checkIfExists: true)
                        .splitCsv( header: true, sep: input_separator )
                        .map{
                            row ->
                                if (row.size() >= 6) {
                                    def line = [:]
                                    line['RGID'] = row.RGID ?: error("ERROR: Missing 'RGID' column!")
                                    line['RGSM'] = row.RGSM ?: error("ERROR: Missing 'RGSM' column!")
                                    line['RGLB'] = row.RGLB ?: error("ERROR: Missing 'RGLB' column!")
                                    line['Lane'] = row.Lane ?: error("ERROR: Missing 'Lane' column!")
                                    line['Sex'] = row.Sex ?: ""
                                    line['Read1File'] = row.Read1File ? file(row.Read1File, checkIfExists: true) : error("ERROR: Missing or invalid 'Read1File' file!")
                                    line['Read2File'] = row.Read2File ? file(row.Read2File, checkIfExists: true) : error("ERROR: Missing or invalid 'Read2File' file!")

                                    def meta = [:]
                                    meta['id'] = row.RGSM
                                    meta['sex'] = row.Sex

                                    return [ meta, line ]
                                } else {
                                    error("Input samplesheet requires at least 6 columns but received ${row.size()}.")
                                }
                        }
                        .tap{ ch_input_header }

    // Create FastQ list with required columns
    ch_fastq_list = ch_input_header
                        .first()
                        .map{ meta, data -> data.keySet().join(',') }
                        .concat( ch_parse_input.map{ meta, data -> data.values().join(',') } )
                        .collectFile(
                            name: "fastq_list.csv",
                            newLine: true,
                            sort: false
                        )

    // Sample channel with meta information and new FastQ list
    ch_samples = ch_parse_input
                    .map{ meta, data -> meta }
                    .combine( ch_fastq_list )

    emit:
    samples = ch_samples // channel: [ val(meta), path(file) ]

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
