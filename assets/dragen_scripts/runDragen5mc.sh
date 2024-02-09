#!/bin/bash

REF=$(readlink -f ~/Active/spencerlab/refdata/hg38/hg38.p13.patched.dragen4.0.5mc.tar.gz)
FC=$1
LANE=$3
BC=$2
SAM=$4

FQ1=$5
FQ2=$6

OUTDIR=/storage1/fs1/dspencer/Active/spencerlab/data/work_orders/2869430_2869592/processed/$SAM

REFDIR=/storage1/fs1/dspencer/Active/spencerlab/refdata/hg38/dragen_ref #/staging/tmp/ref$SAM
TMPDIR=/staging/dhstmp

mkdir $OUTDIR && \
/opt/edico/bin/dragen --enable-methylation-calling true --methylation-protocol directional --ref-dir $REFDIR \
		      --RGID $FC.$LANE.$BC --RGLB $SAM.$BC --RGPL illumina --RGPU $FC.$LANE --RGSM $SAM \
		      --enable-sort true --enable-duplicate-marking true --methylation-generate-cytosine-report true \
		      --intermediate-results-dir $TMPDIR \
		      -1 $FQ1 -2 $FQ2 --output-directory $OUTDIR --output-file-prefix $SAM

