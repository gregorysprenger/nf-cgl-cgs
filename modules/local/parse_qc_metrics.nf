process PARSE_QC_METRICS {
    label 'process_low'

    container 'docker.io/gregorysprenger/pandas-excel:v2.0.1'

    input:
    path(mgi_worksheet)
    path(single_sample_metrics)
    path(joint_sample_metrics)

    output:
    path("*.xlsx")      , emit: qc_metrics
    path("versions.yml"), emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix          = task.ext.prefix
    def mgi_samplesheet = mgi_worksheet ? "--mgi_worksheet ${mgi_worksheet}" : ""
    """
    parse_qc_metrics.py \\
        ${mgi_samplesheet} \\
        --input_dir \$PWD \\
        --outdir \$PWD \\
        --prefix ${prefix}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """
}
