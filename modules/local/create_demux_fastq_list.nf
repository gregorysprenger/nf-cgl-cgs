process CREATE_DEMUX_FASTQ_LIST {
    tag "${task.ext.prefix.id}"
    label 'process_single'

    container 'dockerreg01.accounts.ad.wustl.edu/cgl/pandas-excel@sha256:1958093220d5785115b73f69e0894b366f75fac646131e5394972ae68d9e4202'

    input:
    tuple val(meta), path(fastq_lists, stageAs: "fastq_lists/fastq_list_*.csv")

    output:
    path("fastq_list.csv"), emit: fastq_list
    path("versions.yml")  , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    create_demux_fastq_list.py \\
        --fastq_lists fastq_lists/*.csv \\
        --outdir .

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """

    stub:
    """
    head -n 1 \$(ls fastq_lists/*.csv | head -n 1) > fastq_list.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """
}
