process DRAGEN_GERMLINE_WGS {
    tag "${meta.sample_id}"
    label 'dragen'
    publishDir "${params.outdir}/${meta.sample_id}_$type", mode: 'copy'

    input:
    tuple val(meta), path(files)
    val(type)

    output:
    tuple val(meta), path ("${meta.sample_id}*"), emit: dragen_output
    path "versions.yml",    emit: versions

    script:
    def input = ""
    if (type == 'fastq' || type == 'mgi_fastq') {
        def read1 = files.find { it.name.contains("R1") }
        def read2 = files.find { it.name.contains("R2") }
        def rgid  = meta.flowcell_id + '.' + meta.index_sequence + '.' + meta.lane_number
        def rglb  = meta.library_name + '.' + meta.index_sequence
        def rgpu  = meta.flowcell_id + '.' + meta.lane_number
        def rgsm  = meta.library_name
        input = "--RGID $rgid --RGLB $rglb --RGPL illumina --RGPU $rgpu --RGSM $rgsm -1 $read1 -2 $read2 "
    }
    if (type == 'fastq_list') {
        input = "--fastq-list ${files} --fastq-list-sample-id ${meta.sample_id}"
    }
    if (type == 'cram') {
        input = "--tumor-cram-input ${files}"
    }
    if (type == 'bam') {
        input = "--tumor-bam-input ${files}"
    }
    """
    /opt/edico/bin/dragen -r ${params.reference} \\
      ${input} \\
      --read-trimmers adapter \\
      --trim-adapter-read1 ${params.read1_adapter} \\
      --trim-adapter-read2 ${params.read2_adapter} \\
      --enable-map-align true \\
      --enable-map-align-output true \\
      --enable-bam-indexing true \\
      --enable-duplicate-marking true \\
      --qc-coverage-ignore-overlaps=true \\
      --gc-metrics-enable=true \\
      --enable-variant-caller true \\
      --vc-combine-phased-variants-distance 3 \\
      --dbsnp ${params.dbsnp} \\
      --enable-sv true \\
      --sv-output-contigs true \\
      --sv-hyper-sensitivity true \\
      --enable-cnv true \\
      --cnv-enable-self-normalization true \\
      --output-format CRAM \\
      --intermediate-results-dir /staging/intermediate-results-dir \\
      --output-file-prefix ${meta.sample_id}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def input = ""
    if (type == 'fastq' || type == 'mgi_fastq') {
        def read1 = files.find { it.name.contains("R1") }
        def read2 = files.find { it.name.contains("R2") }
        def rgid  = meta.flowcell_id + '.' + meta.index_sequence + '.' + meta.lane_number
        def rglb  = meta.library_name + '.' + meta.index_sequence
        def rgpu  = meta.flowcell_id + '.' + meta.lane_number
        def rgsm  = meta.library_name
        input = "--RGID $rgid --RGLB $rglb --RGPL illumina --RGPU $rgpu --RGSM $rgsm -1 $read1 -2 $read2 "
    }
    if (type == 'fastq_list') {
        input = "--fastq-list ${files} --fastq-list-sample-id ${meta.sample_id}"
    }
    if (type == 'cram') {
        input = "--tumor-cram-input ${files}"
    }
    if (type == 'bam') {
        input = "--tumor-bam-input ${files}"
    }
    """
    echo ${input} > ${meta.sample_id}.txt
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(cat ${projectDir}/assets/test_data/dragen_version.txt)
    END_VERSIONS
    """
}
