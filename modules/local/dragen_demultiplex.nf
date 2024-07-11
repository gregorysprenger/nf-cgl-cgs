process DRAGEN_DEMULTIPLEX {
    tag "$params.batch"
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
    --output-directory \$(realpath ${demuxdir})

    cp ${rundir}/RunParameters.xml ${demuxdir}/Reports/
    FLOWCELL=`grep Flowcell ${rundir}/RunInfo.xml | head -n 1 | cut -d '>' -f 2 | cut -d '<' -f 1`
    awk -v FC=\$FLOWCELL -v FS=',' -v OFS=',' '{ if (NR==1){ print \$0; } else { print FC"."\$1,\$2,\$2"."substr(\$1,1,length(\$1)-2),\$4,\$5,\$6; } }' ${demuxdir}/Reports/fastq_list.csv > fastq_list.csv

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
