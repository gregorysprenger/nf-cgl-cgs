/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CONVERT_XLSX_TO_CSV } from '../../modules/local/convert_xlsx_to_csv'
include { UPDATE_SAMPLE_NAME  } from '../../modules/local/update_sample_name'

/*
========================================================================================
    SUBWORKFLOW TO CHECK INPUTS
========================================================================================
*/

workflow INPUT_CHECK {

    take:
    mgi_samplesheet  //  string: Path to input MGI samplesheet
    ch_fastq_list    //  channel: [ path(file) ]
    ch_bam_cram_list //  channel: [ path(file) ]

    main:
    ch_versions         = Channel.empty()
    ch_samples          = Channel.empty()
    ch_bam_cram_samples = Channel.empty()
    ch_mgi_samplesheet  = Channel.empty()

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
        if (samplesheet.map{ hasExtension(it, '.xlsx') }) {
            CONVERT_XLSX_TO_CSV (
                samplesheet
            )
            ch_versions        = ch_versions.mix(CONVERT_XLSX_TO_CSV.out.versions)
            ch_mgi_samplesheet = ch_mgi_samplesheet.mix(CONVERT_XLSX_TO_CSV.out.csv)

        } else if (samplesheet.any{ hasExtension(it, '.csv') || hasExtension(it, '.tsv') }) {
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
                            def data = parseInputList(it)
                            def requiredColumns = ['RGID', 'RGSM', 'RGLB', 'Lane', 'Read1File', 'Read2File']
                            data.collect{
                                if (!it.keySet().containsAll(requiredColumns)) {
                                    error("Missing required columns in input FastQ list!")
                                }

                                def R1 = file(it['Read1File'], checkIfExists: true)
                                def R2 = file(it['Read2File'], checkIfExists: true)

                                // Ensure FastQ size > min_fastq_size unless validation samples are used
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
                                def matcher = it.RGSM =~ regexPattern
                                def acc = matcher.find() ? matcher.group(0) : it.RGSM
                                def meta = [
                                    'id'  : it.RGSM,
                                    'acc' : acc,
                                    'RGSM': it.RGSM
                                ]

                                [ meta.acc, meta, [ R1, R2 ] ]
                            }
                        }
                        .groupTuple()
                        .combine(
                            ch_fastq_list
                                .filter{ it != [] }
                                .map{
                                    def data = parseInputList(it)
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
                        .map{ id, meta, reads, fastq_list -> [ meta[0], reads.flatten(), fastq_list, [] ] }
                )

    /*
    ================================================================================
                        Process input BAM/CRAM list
    ================================================================================
    */

    ch_bam_cram_samples = ch_bam_cram_samples.mix(
                            ch_bam_cram_list
                                .filter{ it != [] }
                                .flatMap{
                                    def data = parseInputList(it)

                                    // Verify CRAM reference file is provided if a CRAM file is in the input samplesheet
                                    if (data.any{ row -> row.containsKey('File') && hasExtension(row['File'], '.cram') } && !params.cram_reference) {
                                        error("A CRAM reference file must be provided when using a CRAM file as input.")
                                    }

                                    def requiredColumns = ['ID', 'File']
                                    data.collect{
                                        if (!it.keySet().containsAll(requiredColumns)) {
                                            error("Missing required columns in input BAM/CRAM list!")
                                        }

                                        def alignment_file = file(it['File'], checkIfExists: true)

                                        // Ensure BAM/CRAM size > min_bam_cram_size unless validation samples are used
                                        if (!params.validation_samples) {
                                            def MIN_BAM_CRAM_SIZE_BYTES = params.min_bam_cram_size * 1024 * 1024
                                            if (alignment_file.size() < MIN_BAM_CRAM_SIZE_BYTES) {
                                                error("BAM/CRAM file '${alignment_file.name}' is ${alignment_file.size()} bytes, less than ${params.min_bam_cram_size}MB minimum!")
                                            }
                                        }

                                        if (hasExtension(alignment_file, '.bam') || hasExtension(alignment_file, '.cram')) {
                                            [ ["id": it['ID'], "acc": it['ID']], alignment_file ]
                                        } else {
                                            error("Input file is not a BAM or CRAM file.")
                                        }
                                    }
                                }
                        )

    UPDATE_SAMPLE_NAME (
        ch_bam_cram_samples
    )
    ch_versions = ch_versions.mix(UPDATE_SAMPLE_NAME.out.versions)

    ch_samples = ch_samples.mix(
        UPDATE_SAMPLE_NAME.out.updated_alignment.map{ meta, alignment_file -> [ meta, [], [], alignment_file ] }
    )

    emit:
    samples         = ch_samples          // channel: [ val(sample_info), path(reads), path(fastq_list), path(alignment_file) ]
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

// Parse FastQ or BAM/CRAM list
def parseInputList(file) {
    def separator = file.endsWith("tsv") ? '\t' : ','
    def lines = file.readLines()
    def headers = lines.first().split(separator)
    lines.drop(1).collect{ line ->
        [headers, line.split(separator)].transpose().collectEntries{ it }
    }
}
