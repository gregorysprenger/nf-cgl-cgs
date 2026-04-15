process GET_SAMPLE_METADATA {
    tag "${task.ext.prefix.id}"
    label 'process_low'
    label 'database_metadata'

    container 'docker.io/apldx/ubuntu-jammy-mamba3.12-mopath-wf@sha256:2d2afaeba019b194728d23324193f7137a5656f02c053eea0b99bfad09d1e55b'

    input:
    val(sample_names)

    output:
    path("${task.ext.prefix.id}.csv"), emit: metadata
    path("versions.yml")             , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix  = task.ext.prefix
    def samples = sample_names.collect{ "--filter-values '" + it + "'" }.join(' ')
    """
    query_database.py \
        --columns Sex \
        --filter-col SpcNum \
        --output "${prefix.id}.csv" \
        --table FranklinOrder \
        ${samples}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix
    """
    touch "${prefix.id}.csv"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | awk '{print \$2}')
    END_VERSIONS
    """
}
