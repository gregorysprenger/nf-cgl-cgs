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
        def dateMatch = params.batch_name.find(/(\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01]))/)
        if (dateMatch) {
            def runDirName   = new File(params.illumina_rundir).name
            def dateInRunDir = runDirName.split('_').any{ it.contains(dateMatch) }

            if (!dateInRunDir) {
                error "Date in batch name ('${dateMatch}') not found in Illumina run directory name ('${runDirName}'). Run directory parts checked: ${runDirName.split('_').join(', ')}. If this is a validation run, please set '--validation_samples' to true."
            }
        } else {
            error "Could not find an 8-digit date (YYYYMMDD) in '--batch_name ${params.batch_name}'. If this is a validation run, please set '--validation_samples' to true"
        }
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

    // Process the fastq list to create samples channel and the updated fastq list for alignment
    ch_processed_fastq = ch_fastq_list
                            .filter{ it.size() > 0 }
                            .flatMap{ fastq_list_file ->
                                def data = parseFastqList(fastq_list_file)
                                def requiredColumns = ['RGID', 'RGSM', 'RGLB', 'Lane', 'Read1File', 'Read2File']
                                data.collect{ row ->
                                    if (!row.keySet().containsAll(requiredColumns)) {
                                        error("Missing required columns in input FastQ list!")
                                    }

                                    def R1 = file(row['Read1File'], checkIfExists: true)
                                    def R2 = file(row['Read2File'], checkIfExists: true)

                                    if (!params.validation_samples) {
                                        def MIN_FASTQ_SIZE_BYTES = params.min_fastq_size * 1024 * 1024
                                        if (R1.size() < MIN_FASTQ_SIZE_BYTES) {
                                            error("FastQ file '${R1.name}' is ${R1.size()} bytes, less than ${params.min_fastq_size}MB minimum!")
                                        }
                                        if (R2.size() < MIN_FASTQ_SIZE_BYTES) {
                                            error("FastQ file '${R2.name}' is ${R2.size()} bytes, less than ${params.min_fastq_size}MB minimum!")
                                        }
                                    }

                                    def regexPattern = /\w\d{2}-\d+/
                                    def matcher = row.RGSM =~ regexPattern
                                    def acc = matcher.find() ? matcher.group(0) : row.RGSM
                                    def meta = ['id': row.RGSM, 'acc': acc, 'RGSM': row.RGSM]

                                    // Create a new map for the updated fastq list to ensure column order
                                    def updated_row = [
                                        'RGID': row.RGID,
                                        'RGSM': row.RGSM,
                                        'RGLB': row.RGLB,
                                        'Lane': row.Lane,
                                        'Read1File': "fastq_files/${R1.name}",
                                        'Read2File': "fastq_files/${R2.name}"
                                    ]

                                    [ meta, [R1, R2], updated_row ]
                                }
                            }
                            .multiMap{ meta, reads, updated_row ->
                                samples: [ meta, reads ]
                                csv_rows: updated_row
                            }

    ch_updated_fastq_list = ch_processed_fastq.csv_rows
                                .collect()
                                .map{ rows ->
                                    if (rows.isEmpty()) return  [ [], [] ]
                                    def header = rows[0].keySet().join(',')
                                    def content = rows.collect{ it.values().join(',') }.join('\n')
                                    [ [header], [content] ]
                                }
                                .filter{ it != [ [], [] ] }
                                .flatten()
                                .collectFile(
                                    newLine : true,
                                    sort    : 'index',
                                    storeDir: "${workflow.workDir}",
                                    name    : "updated_fastq_list.csv",
                                )

    ch_samples = ch_processed_fastq.samples
                    .map{ meta, reads -> [ meta.id, meta, reads ] }
                    .groupTuple()
                    .map{ id, metas, reads -> [ metas[0], reads.flatten() ] }
                    .combine(ch_updated_fastq_list)

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
    if (lines.isEmpty()) {
        return []
    }
    def headers = lines.first().split(separator) as List
    lines.drop(1).collect{ line ->
        [headers, line.split(separator)].transpose().collectEntries{ it }
    }
}
