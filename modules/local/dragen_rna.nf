process DRAGEN_RNA {
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
        def rgsm  = meta.library_name
        input = "--tumor-fastq1 $read1 --tumor-fastq2 $read2 --RGID-tumor $rgid --RGLB-tumor $rglb --RGSM-tumor $rgsm "
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
                -a ${params.annotation_file} --intermediate-results-dir /staging/intermediate-results-dir \\
                --enable-map-align true --enable-sort true --enable-bam-indexing true --enable-map-align-output true --enable-duplicate-marking true --rrna-filter-enable true \\
                --output-format CRAM --enable-rna-quantification true --enable-rna-gene-fusion true \\
                --enable-variant-caller true \\
                --enable-down-sampler true --down-sampler-reads 100000000 \\
                ${input} \\
                --output-directory ./ --force --output-file-prefix ${meta.sample_id}
                
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
        def rgsm  = meta.library_name
        input = "--tumor-fastq1 $read1 --tumor-fastq2 $read2 --RGID-tumor $rgid --RGLB-tumor $rglb --RGSM-tumor $rgsm "
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
