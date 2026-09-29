process CONCATENATE_FASTQ_FILES {
    tag "${meta.id}"
    label 'process_low'

    container 'docker.io/ubuntu:noble'

    input:
    tuple val(meta), path(fastq_list_reads, stageAs: "fastq_list_reads/*"), path(demux_reads, stageAs: "demux_reads/*")

    output:
    tuple val(meta), path("${meta.id}_R{1,2}.fastq.gz"), path("${meta.id}_fastq_list.csv"), emit: samples
    path("versions.yml")                                                                   , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def r1_files = ([fastq_list_reads, demux_reads].flatten().findAll{ it.name =~ /_R1/ }.sort{ it.name }).join(' ')
    def r2_files = ([fastq_list_reads, demux_reads].flatten().findAll{ it.name =~ /_R2/ }.sort{ it.name }).join(' ')
    """
    cat ${r1_files} > ${meta.id}_R1.fastq.gz
    cat ${r2_files} > ${meta.id}_R2.fastq.gz

    printf "RGID,RGSM,RGLB,Lane,Read1File,Read2File\\n" > ${meta.id}_fastq_list.csv
    printf "${meta.id},${meta.id},UnknownLibrary,1,fastq_files/${meta.id}_R1.fastq.gz,fastq_files/${meta.id}_R2.fastq.gz\\n" >> ${meta.id}_fastq_list.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cat: \$(cat --version | head -1 | sed 's/cat (GNU coreutils) //')
    END_VERSIONS
    """

    stub:
    """
    echo "" | gzip > ${meta.id}_R1.fastq.gz
    echo "" | gzip > ${meta.id}_R2.fastq.gz

    printf "RGID,RGSM,RGLB,Lane,Read1File,Read2File\\n" > ${meta.id}_fastq_list.csv
    printf "${meta.id},${meta.id},UnknownLibrary,1,fastq_files/${meta.id}_R1.fastq.gz,fastq_files/${meta.id}_R2.fastq.gz\\n" >> ${meta.id}_fastq_list.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cat: \$(cat --version | head -1 | sed 's/cat (GNU coreutils) //')
    END_VERSIONS
    """
}
