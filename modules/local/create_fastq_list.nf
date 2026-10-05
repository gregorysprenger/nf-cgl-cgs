process CREATE_FASTQ_LIST {
    tag "${meta.id}"
    label 'process_single'

    container 'dockerreg01.accounts.ad.wustl.edu/cgl/pandas-excel@sha256:1958093220d5785115b73f69e0894b366f75fac646131e5394972ae68d9e4202'

    input:
    tuple val(meta), val(reads), val(rows)

    output:
    tuple val(meta), val(reads), path("*_fastq_list.csv"), val([]), emit: samples
    path("versions.yml")                                           , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix  = meta.id.toString().replaceAll(/[^A-Za-z0-9._-]/, '_')
    def columns = rows.collectMany{ it.keySet() as List }.unique()
    def lines   = [ columns.join(',') ] + rows.collect{ row -> columns.collect{ row[it] ?: '' }.join(',') }
    def input   = lines.collect{ "'" + it.replace("'", "'\\''") + "'" }.join(' \\\n            ')
    """
    create_fastq_list.py \\
        --output ${prefix}_fastq_list.csv \\
        --rows \\
        ${input}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """

    stub:
    def prefix = meta.id.toString().replaceAll(/[^A-Za-z0-9._-]/, '_')
    """
    echo "RGID,RGSM,RGLB,Lane,Read1File,Read2File" > ${prefix}_fastq_list.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """
}
