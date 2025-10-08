process PARSE_QC_METRICS {
    tag "${task.ext.prefix.id}"
    label 'process_low'

    container 'docker.io/gregorysprenger/pandas-excel:v2.2.2'

    input:
    path(samplesheet)
    path(single_sample_metrics), stageAs: "single_sample_metrics/"
    path(joint_sample_metrics) , stageAs: "joint_sample_metrics/"

    output:
    path("*.xlsx")      , emit: qc_metrics
    path("*Genoox.xlsx"), emit: genoox_metrics
    path("versions.yml"), emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix
    """
    # Remove single sample metrics if joint called metrics
    if [[ -d joint_sample_metrics ]]; then
        find -L joint_sample_metrics \\
            -type f \\
            -exec basename "{}" \\; \\
            | while read file; do
                if [[ -f "single_sample_metrics/\${file}" ]]; then
                    rm "single_sample_metrics/\${file}"
                fi
            done
    fi

    # Create metric summary files
    parse_qc_metrics.py \\
        --mgi_worksheet ${samplesheet} \\
        --inputdir \$PWD \\
        --outdir \$PWD \\
        --prefix ${prefix.id}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix
    """
    # Remove single sample metrics if joint called metrics
    if [[ -d joint_sample_metrics ]]; then
        find -L joint_sample_metrics \\
            -type f \\
            -exec basename "{}" \\; \\
            | while read file; do
                if [[ -f "single_sample_metrics/\${file}" ]]; then
                    rm "single_sample_metrics/\${file}"
                fi
            done
    fi

    # Create metric summary files
    parse_qc_metrics.py \\
        --mgi_worksheet ${samplesheet} \\
        --inputdir \$PWD \\
        --outdir \$PWD \\
        --prefix ${prefix.id}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """
}
