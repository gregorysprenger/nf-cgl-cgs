process DRAGEN_ALIGN {
    tag "${meta.id}"
    label 'dragen'
    label 'dragenalign'
    container "${ext.dragen_aws_image}" ?: "${params.dragen_container}"
    publishDir "$params.outdir/${meta.id}/", saveAs: { filename -> filename == "versions.yml" ? null : filename.split('/')[1] }, mode:'copy'

    input:
    tuple val(meta), val(type), path("*")
    tuple val(dragen_inputs), path("*", stageAs: 'inputs/*')

    output:
    tuple val(meta), path ("dragen/*"), emit: dragen_output
    path "versions.yml",    emit: versions

    script:
    def input = ""
    if (type == 'fastq_list') {
        input = "--fastq-list fastq_list.csv --fastq-list-sample-id ${meta.id}"

    } else if (type == 'cram') {
        input = "--cram-input ${meta.cram}"
    }
    if (type == 'bam') {
        input = "--bam-input ${meta.bam}"
    }

    def intermediate_dir = task.ext.intermediate_dir ? "--intermediate-results-dir ${task.ext.intermediate_dir}" : ""
    def args_license = task.ext.dragen_license_args ?: ''
    def specified_sex = meta.sex != null ? "--sample-sex ${meta.sex}" : ""

    def dragen_args = "--enable-variant-caller true --dbsnp inputs/${dragen_inputs.dbsnp} --enable-sv true --sv-output-contigs true --enable-cnv true --cnv-enable-self-normalization true --enable-cyp2b6 true --enable-cyp2d6 true --enable-gba true --enable-smn true --repeat-genotype-enable true"

    // if a family_id is given, then we are running a trio or family analysis and need gVCF output
    if (meta.family_id != null) {
        dragen_args += " --vc-emit-ref-confidence GVCF"
    }

    """
    mkdir dragen && \\
    /opt/edico/bin/dragen -r inputs/${dragen_inputs.reference} ${specified_sex} ${input} ${intermediate_dir} ${args_license}\\
                --enable-map-align true \\
                --enable-sort true \\
                --enable-bam-indexing true \\
                --enable-map-align-output true \\
                --qc-coverage-ignore-overlaps=true \\
                --gc-metrics-enable true \\
                --enable-duplicate-marking ${params.mark_duplicates} \\
                --read-trimmers adapter \\
                --trim-adapter-read1 inputs/${dragen_inputs.dragen_adapter1} \\
                --trim-adapter-read2 inputs/${dragen_inputs.dragen_adapter2} \\
                --output-format ${params.alignment_file_format} \\
                --output-directory ./dragen --force --output-file-prefix ${meta.id} ${dragen_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
        def input = ""
    if (type == 'fastq_list') {
        input = "--fastq-list fastq_list.csv --fastq-list-sample-id ${meta.id}"

    } else if (type == 'cram') {
        input = "--cram-input ${meta.cram}"
    }
    if (type == 'bam') {
        input = "--bam-input ${meta.bam}"
    }

    def intermediate_dir = task.ext.intermediate_dir ? "--intermediate-results-dir ${task.ext.intermediate_dir}" : ""
    def args_license = task.ext.dragen_license_args ?: ''
    def specified_sex = meta.sex != null ? "--sample-sex ${meta.sex}" : ""

    def dragen_args = "--enable-variant-caller true --dbsnp inputs/${dragen_inputs.dbsnp} --enable-sv true --sv-output-contigs true --enable-cnv true --cnv-enable-self-normalization true --enable-cyp2b6 true --enable-cyp2d6 true --enable-gba true --enable-smn true --repeat-genotype-enable true"

    // if a family_id is given, then we are running a trio or family analysis and need gVCF output
    if (meta.family_id != null) {
        dragen_args += " --vc-emit-ref-confidence GVCF"
    }
    """
    mkdir dragen && \\
    echo /opt/edico/bin/dragen -r inputs/${dragen_inputs.reference} ${input} ${intermediate_dir} ${args_license}\\
                --enable-map-align true \\
                --enable-sort true \\
                --enable-bam-indexing true \\
                --enable-map-align-output true \\
                --qc-coverage-ignore-overlaps=true \\
                --gc-metrics-enable true \\
                --enable-duplicate-marking ${params.mark_duplicates} \\
                --read-trimmers adapter \\
                --trim-adapter-read1 inputs/${dragen_inputs.dragen_adapter1} \\
                --trim-adapter-read2 inputs/${dragen_inputs.dragen_adapter2} \\
                --output-format ${params.alignment_file_format} \\
                --output-directory ./dragen --force --output-file-prefix ${meta.id} ${dragen_args} > ./dragen/${meta.id}.txt

    for i in ${projectDir}/assets/stub/dragen_path/*; do
        cp $i ./dragen/
    done
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(cat $projectDir/assets/stub/versions/dragen_version.txt)
    END_VERSIONS
    """
}
