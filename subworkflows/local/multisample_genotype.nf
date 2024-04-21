include { DRAGEN_JOINT_GENOTYPE              } from '../../modules/local/dragen_joint_genotype.nf'

workflow MULTISAMPLE_GENOTYPE {
    take:
    ch_genotype_input
    dragen_input

    main:

    ch_gvcfs            = Channel.empty()
    ch_cnvfiles         = Channel.empty()
    ch_bams             = Channel.empty()

    ch_genotype_input
    .map { family_id, pedfile, gvcfs, cnvfiles, svfiles ->
        [ family_id, pedfile, gvcfs ]
    }
    .set { ch_gvcfs }

    DRAGEN_JOINT_GENOTYPE(ch_gvcfs, dragen_input)

    ch_genotype_input
    .map { family_id, pedfile, gvcfs, cnvfiles, svfiles ->
        [ family_id, pedfile, cnvfiles ]
    }
    .set { ch_cnvfiles }

    DRAGEN_JOINT_CNV(ch_cnvfiles, dragen_input)

    ch_genotype_input
    .map { family_id, pedfile, gvcfs, cnvfiles, svfiles ->
        [ family_id, pedfile, svfiles ]
    }
    .set { ch_svfiles }

    DRAGEN_JOINT_SV(ch_svfiles, dragen_input)

    emit:

}
