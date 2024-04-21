process DRAGEN_JOINT_SV {
    tag "${meta.id}"
    label 'dragen'
    label 'dragenalign'
    container "${ext.dragen_aws_image}" ?: "${params.dragen_container}"

//    publishDir "$params.outdir/${meta.id}/", saveAs: { filename -> filename == "versions.yml" ? null : filename.split('/')[1] }, mode:'copy'

    input:
    tuple val(meta), val(pedfile), path("*")
    tuple val(dragen_inputs), path("*", stageAs: 'inputs/*')

    output:
    tuple val(meta), path ("dragen/*"), emit: dragen_output
    path "versions.yml",    emit: versions

    script:

    """
    echo ${pedfile} > ${meta.family_id}.ped && mkdir dragen && \\
    /opt/edico/bin/dragen -r inputs/${dragen_inputs.reference} ${intermediate_dir} ${args_license}\\
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

    """
    echo ${pedfile} > ${meta.family_id}.ped && mkdir dragen && \\
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
