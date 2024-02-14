workflow DEMUX {
    take:
    mastersheet
    rundir
    demuxdir

    main:
    ch_versions = Channel.empty()

    // make demux samplesheet
    MAKE_DEMUX_SAMPLESHEET(mastersheet, rundir)
    ch_versions = ch_versions.mix(MAKE_DEMUX_SAMPLESHEET.out.versions)

    // do demux
    DRAGEN_DEMUX(MAKE_DEMUX_SAMPLESHEET.out.samplesheet, rundir, demuxdir)
    ch_versions = ch_versions.mix(DRAGEN_DEMUX.out.versions)

    DRAGEN_DEMUX.out.fastqlist
    .splitCsv ( header:true, sep:',', quote:'"' )
    .map { row -> [row.RGSM, [row.RGID, row.RGLB, row.Lane, file(row.Read1File), file(row.Read2File) ]]
    }
    .set { ch_fastqs }

    ch_fastqs
    .map { it -> [ it[0] ] }
    .combine(MAKE_DEMUX_SAMPLESHEET.out.runinfo)
    .set { ch_runinfo }

    emit:
    fastqlist = ch_fastqs.join(ch_runinfo)
    versions = ch_versions

}

process MAKE_DEMUX_SAMPLESHEET {
    label 'process_low'
    container "ghcr.io/dhslab/docker-python3:231224"

    input:
    path mastersheet
    path rundir

    output:
    path("*.demux_samplesheet.csv"), emit: samplesheet
    path("*.runinfo.csv"), emit: runinfo
    path "versions.yml", emit: versions

    script:
    """
    prepare_dragen_demux.py -r ${rundir} -s ${mastersheet}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(prepare_dragen_demux.py --version)
    END_VERSIONS
    """

    stub:
    """
    touch STUB.demux_samplesheet.csv
    touch STUB.runinfo.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(prepare_dragen_demux.py --version)
    END_VERSIONS
    """
}

process DRAGEN_DEMUX {
    label 'dragen'
    container "${params.dragen_container}"

    input:
    path samplesheet
    path rundir
    path demuxdir

    output:
    path ('fastq_list.csv'), emit: fastqlist
    path "versions.yml",    emit: versions

    script:
    def first_tile = params.bcl_first_tile ? " --first-tile-only true" : ""
    """
    /opt/edico/bin/dragen --bcl-conversion-only true --bcl-only-matched-reads true --strict-mode true${first_tile} \\
    --sample-sheet ${samplesheet} --bcl-input-directory ${rundir} \\
    --output-directory \$(realpath ${demuxdir}) > ./demux_log.txt && \\
    cp demux_log.txt ${demuxdir}/Reports/ && \\
    cp ${rundir}/RunParameters.xml ${demuxdir}/Reports/ && \\
    cp ${demuxdir}/Reports/fastq_list.csv fastq_list.csv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(/opt/edico/bin/dragen --version | tail -n 1 | cut -d ' ' -f 3)
    END_VERSIONS
    """

    stub:
    """
    cp ${projectDir}/assets/stub/demux_fastq/Reports/fastq_list.csv .

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        dragen: \$(cat ${projectDir}/assets/stub/versions/dragen_version.txt)
    END_VERSIONS
    """

}