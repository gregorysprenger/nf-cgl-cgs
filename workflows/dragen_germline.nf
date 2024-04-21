/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PRINT PARAMS SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { paramsSummaryLog; paramsSummaryMap } from 'plugin/nf-validation'

def logo = NfcoreTemplate.logo(workflow, params.monochrome_logs)
def citation = '\n' + WorkflowMain.citation(workflow) + '\n'
def summary_params = paramsSummaryMap(workflow)

// Print parameter summary log to screen
log.info logo + paramsSummaryLog(workflow) + citation

WorkflowDragengermline.initialise(params, log)

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CONFIG FILES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

ch_multiqc_config          = Channel.fromPath("$projectDir/assets/multiqc_config.yml", checkIfExists: true)
ch_multiqc_custom_config   = params.multiqc_config ? Channel.fromPath( params.multiqc_config, checkIfExists: true ) : Channel.empty()
ch_multiqc_logo            = params.multiqc_logo   ? Channel.fromPath( params.multiqc_logo, checkIfExists: true ) : Channel.empty()
ch_multiqc_custom_methods_description = params.multiqc_methods_description ? file(params.multiqc_methods_description, checkIfExists: true) : file("$projectDir/assets/methods_description_template.yml", checkIfExists: true)

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// SUBWORKFLOW: Consisting of a mix of local and nf-core/modules
//
include { SAMPLE_INPUT_CHECK    } from '../subworkflows/local/sample_input_check.nf'
include { MULTISAMPLE_GENOTYPE  } from '../subworkflows/local/multisample_genotype.nf'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT NF-CORE MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// MODULE: Installed directly from nf-core/modules

//
include { DRAGEN_ALIGN                } from '../modules/local/dragen_align.nf'
include { FASTQC                      } from '../modules/nf-core/fastqc/main'
include { MULTIQC                     } from '../modules/nf-core/multiqc/main'
include { CUSTOM_DUMPSOFTWAREVERSIONS } from '../modules/nf-core/custom/dumpsoftwareversions/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// This function 'stages' a set of files defined by a map of key:filepath pairs.
// It returns a tuple: a map of key:filename pairs and list of file paths.
// This can be used to generate a value Channel that can be used as input to a process
// that accepts a tuple val(map), path("*") so map.key refers to the appropriate linked file.
def stageFileset(Map filePathMap) {
    def basePathMap = [:]
    def filePathsList = []

    filePathMap.each { key, value ->
        if (value != null) {
            def filepath = file(value)
            if (filepath.exists()) {
                // Add basename and key to the map
                basePathMap[key] = value.split('/')[-1]
                // Add file path to the list
                filePathsList << filepath
            } else {
                println "Warning: File at '${value}' for key '${key}' does not exist."
            }
        }
    }
    return [basePathMap, filePathsList]
}

// If MGI samplesheet is used, we need to set the 
// data path because only files are given. This sets the 
// data path to the samplesheet directory, or the data_path parameter.
def data_path = ""
def mastersheet = params.mastersheet
if (params.mgi == true) {
    data_path = new File(params.mastersheet).parentFile.absolutePath
} else if (params.data_path != null){
    data_path  = params.data_path
}

// Info required for completion email and summary
def multiqc_report = []

workflow DRAGEN_GERMLINE {

    ch_versions     = Channel.empty()
    ch_input_data      = Channel.empty()

    SAMPLE_INPUT_CHECK(Channel.fromPath(mastersheet), data_path)
    ch_versions = ch_versions.mix(SAMPLE_INPUT_CHECK.out.versions)

    ch_input_data = SAMPLE_INPUT_CHECK.out.input_data

    // this stages all dragen align inputs into an object keyed by the parameter name
    ch_dragen_inputs = Channel.value(stageFileset(params.dragen_inputs))

    DRAGEN_ALIGN(ch_input_data, ch_dragen_inputs)
    ch_versions = ch_versions.mix(DRAGEN_ALIGN.out.ch_versions)

    // Channel operations to prepare joint genotyping
    DRAGEN_ALIGN.out.dragen_output
    .filter { it[0].family_id != null }
    .map { meta, outfiles -> 
        def new_meta = meta.subMap('id','relationship','sex')
        def cnvfiles = outfiles.findAll { it.name.endsWith('.tn.tsv') }
        def gvcfs = outfiles.findAll { it.name.endsWith('.gvcf.gz') || it.name.endsWith('.gvcf.gz.tbi') }
        def svfiles = outfiles.findAll { it.name.endsWith('.bam') || it.name.endsWith('.bam.bai') it.name.endsWith('.cram') || it.name.endsWith('.cram.crai') }

        [meta.family_id, new_meta, gvcfs, cnvfiles, svfiles]
    }
    .groupTuple()
    .map { family_id, meta, gvcfs, cnvfiles, svfiles ->
        // iterate over each item in the list of meta objects

        def proband_sex = ""
        def proband_id = ""
        def father_id = ""
        def mother_id = ""

        meta.each { sample_meta ->            
            if (sample_meta.relationship == 'proband') {
                proband_sex = sample_meta.sex == 'male' ? '1' : '2'
                proband_id_ = sample_meta.id
            } else if (sample_meta.relationship == 'father') {
                father_id = sample_meta.id
            } else if (sample_meta.relationship == 'mother') {
                mother_id = sample_meta.id
            }
        }
        def pedfile = "#Family_ID\tIndividual_ID\tPaternal_ID\tMaternal_ID\tSex\tPhenotype\n"
        pedfile += "${family_id}\t${proband_id}\t${father_id}\t${mother_id}\t${proband_sex}\t2\n"
        pedfile += "${family_id}\t${father_id}\t0\t0\t1\t1\n"
        pedfile += "${family_id}\t${mother_id}\t0\t0\t2\t1\n"

        [ family_id, pedfile, gvcfs, cnvfiles, svfiles ]
    }
    .set { multisample_genotype_input }

//    MULTISAMPLE_GENOTYPE(multisample_genotype_input, ch_dragen_inputs)

    CUSTOM_DUMPSOFTWAREVERSIONS (
        ch_versions.unique().collectFile(name: 'collated_versions.yml')
    )

    //
    // MODULE: MultiQC
    //
    workflow_summary    = WorkflowDragenmultiworkflow.paramsSummaryMultiqc(workflow, summary_params)
    ch_workflow_summary = Channel.value(workflow_summary)

    methods_description    = WorkflowDragenmultiworkflow.methodsDescriptionText(workflow, ch_multiqc_custom_methods_description, params)
    ch_methods_description = Channel.value(methods_description)

    ch_multiqc_files = Channel.empty()
    ch_multiqc_files = ch_multiqc_files.mix(ch_workflow_summary.collectFile(name: 'workflow_summary_mqc.yaml'))
    ch_multiqc_files = ch_multiqc_files.mix(ch_methods_description.collectFile(name: 'methods_description_mqc.yaml'))
    ch_multiqc_files = ch_multiqc_files.mix(CUSTOM_DUMPSOFTWAREVERSIONS.out.mqc_yml.collect())

    MULTIQC (
        ch_multiqc_files.collect(),
        ch_multiqc_config.toList(),
        ch_multiqc_custom_config.toList(),
        ch_multiqc_logo.toList()
    )
    multiqc_report = MULTIQC.out.report.toList()
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    COMPLETION EMAIL AND SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow.onComplete {
    if (params.email || params.email_on_fail) {
        NfcoreTemplate.email(workflow, params, summary_params, projectDir, log, multiqc_report)
    }
    NfcoreTemplate.dump_parameters(workflow, params)
    NfcoreTemplate.summary(workflow, params, log)
    if (params.hook_url) {
        NfcoreTemplate.IM_notification(workflow, params, summary_params, projectDir, log)
    }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
