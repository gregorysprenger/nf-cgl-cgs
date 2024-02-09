process DRAGEN_TUMOR_NORMAL {
    label 'dragen'
    publishDir "${params.outdir}/${normal_meta.sample_id}_$type", mode: 'copy'

    input:
    tuple val(normal_meta), path(normal_files), val(tumor_meta), path(tumor_files)
    val(type)

    output:
    tuple val(normal_meta), path ("${normal_meta.sample_id}*"), emit: dragen_output
    path "versions.yml",    emit: versions

    script:
    def input = ""
    if (type == 'fastq' || type == 'mgi_fastq') {
        def normal_read1 = normal_files.find { it.name.contains("R1") }
        def normal_read2 = normal_files.find { it.name.contains("R2") }
        def tumor_read1 = tumor_files.find { it.name.contains("R1") }
        def tumor_read2 = tumor_files.find { it.name.contains("R2") }
        input = "--fastq-file1 ${normal_read1} --fastq-file2 ${normal_read2} --RGID ${normal_meta.rgid} --RGLB ${normal_meta.rglb} --RGSM ${normal_meta.rgsm} \
        --tumor-fastq1 ${tumor_read1} --tumor-fastq2 ${tumor_read2} --RGID-tumor ${tumor_meta.rgid} --RGLB-tumor ${tumor_meta.rglb} --RGSM-tumor ${tumor_meta.rgsm}"
    }
    if (type == 'fastq_list') {
        input = "--tumor-fastq-list ${tumor_files} --tumor-fastq-list-sample-id ${tumor_meta.sample_id} --fastq-list ${normal_files} --fastq-list-sample-id ${normal_meta.sample_id}"
    }
    if (type == 'cram') {
        input = "--tumor-cram-input ${files}"
    }
    if (type == 'bam') {
        input = "--tumor-bam-input ${files}"
    }
    """
    /opt/edico/bin/dragen -r ${params.reference} \
        ${input} \
        --read-trimmers adapter \
        --trim-adapter-read1 ${params.read1_adapter} \
        --trim-adapter-read2 ${params.read2_adapter} \
        --enable-map-align true \
        --enable-map-align-output true \
        --enable-bam-indexing true \
        --enable-duplicate-marking true \
        --qc-coverage-ignore-overlaps true \
        --gc-metrics-enable true \
        --enable-variant-caller true \
        --vc-enable-liquid-tumor-mode true \
        --vc-combine-phased-variants-distance 3 \
        --dbsnp ${params.dbsnp} \
        --enable-sv true \
        --sv-output-contigs true \
        --sv-hyper-sensitivity true \
        --sv-use-overlap-pair-evidence true \
        --enable-cnv true \
        --cnv-use-somatic-vc-baf true \
        --cnv-somatic-enable-het-calling true \
        --cnv-enable-ref-calls false \
        --enable-variant-annotation true --variant-annotation-assembly GRCh38 --variant-annotation-data ${params.annotation_data} \
        --output-format CRAM \
        --intermediate-results-dir /staging/intermediate-results-dir \
        --output-directory ./ --force \
        --output-file-prefix ${normal_meta.sample_id}
        
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    def input = ""
    if (type == 'fastq' || type == 'mgi_fastq') {
        def normal_read1 = normal_files.find { it.name.contains("R1") }
        def normal_read2 = normal_files.find { it.name.contains("R2") }
        def tumor_read1 = tumor_files.find { it.name.contains("R1") }
        def tumor_read2 = tumor_files.find { it.name.contains("R2") }
        input = "--fastq-file1 ${normal_read1} --fastq-file2 ${normal_read2} --RGID ${normal_meta.rgid} --RGLB ${normal_meta.rglb} --RGSM ${normal_meta.rgsm} \
        --tumor-fastq1 ${tumor_read1} --tumor-fastq2 ${tumor_read2} --RGID-tumor ${tumor_meta.rgid} --RGLB-tumor ${tumor_meta.rglb} --RGSM-tumor ${tumor_meta.rgsm}"
    }
    if (type == 'fastq_list') {
        input = "--fastq-list ${fastqfile} --fastq-list-sample-id ${meta.sample_id}"
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