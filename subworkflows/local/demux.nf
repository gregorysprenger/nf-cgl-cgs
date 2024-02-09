include { MAKE_DEMUX_SAMPLESHEET      } from '../../modules/local/make_demux_samplesheet.nf'
include { DRAGEN_DEMUX                } from '../../modules/local/dragen_demux.nf'

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

