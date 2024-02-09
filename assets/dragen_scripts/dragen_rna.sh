#!/usr/bin/env bash

#bsub -eo Pro_150422.err -oo Pro_150422.log -g /dspencer/dragen -G compute-dspencer -q dragen-4 -M 400G -n 30 -R "span[hosts=1] select[mem>400G] rusage[mem=400G]" -a 'gtac-mgi-dragen(etycksen/dragen4:4.2.4)'

NAME=$1
IN=$2
VCF=$3
OUT=$(readlink -f $4)

TYPE="--tumor-cram-input"

VCFARGS=""

if [[ -e $VCF ]];
then
    VCFARGS="--vc-forcegt-vcf "$VCF
fi

# Check the file extension and assign TYPE variable
if [[ "$IN" == *.bam ]]; then
    TYPE="--tumor-bam-input"
fi

mkdir -p $OUT/$NAME && \
/opt/edico/bin/dragen -r /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen424_hg38 \
		      -a /storage1/fs1/dspencer/Active/spencerlab/refdata/hg38/ensemble/Homo_sapiens.GRCh38.105.chr.sorted.gtf.gz --intermediate-results-dir /staging/intermediate-results-dir \
		      --enable-map-align true --enable-sort true --enable-bam-indexing true --enable-map-align-output true --enable-duplicate-marking true --rrna-filter-enable true \
		      --output-format CRAM --enable-rna-quantification true --enable-rna-gene-fusion true \
		      --enable-variant-caller true $VCFARGS \
              --enable-down-sampler true --down-sampler-reads 100000000 \
		      $TYPE $IN --output-directory $OUT/$NAME --output-file-prefix $NAME
