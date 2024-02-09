#!/bin/bash

#bsub -g /dspencer/adhoc -G compute-dspencer -q dragen-4 -M 350G -n 30 -R "span[hosts=1] select[mem>350G] rusage[mem=350G]" -a 'docker(seqfu/oracle8-dragen-4.0.3:latest)'

/opt/edico/bin/dragen -r /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen424_hg38 \
      --tumor-fastq-list $1 \
      --tumor-fastq-list-sample-id $2 \
      --fastq-list $1 \
      --fastq-list-sample-id $3 \
      --read-trimmers adapter \
      --trim-adapter-read1 /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen_align_inputs/hg38/t2t-chm13_adapter1.fa \
      --trim-adapter-read2 /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen_align_inputs/hg38/t2t-chm13_adapter2.fa \
      --enable-map-align true \
      --enable-map-align-output true \
      --enable-bam-indexing true \
      --enable-duplicate-marking true \
      --qc-coverage-ignore-overlaps true \
      --gc-metrics-enable true \
      --enable-variant-caller true \
      --vc-enable-liquid-tumor-mode true \
      --vc-combine-phased-variants-distance 3 \
      --dbsnp /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen_align_inputs/hg38/dbsnp.vcf.gz \
      --enable-sv true \
      --sv-output-contigs true \
      --sv-hyper-sensitivity true \
      --sv-use-overlap-pair-evidence true \
      --enable-cnv true \
      --cnv-use-somatic-vc-baf true \
      --cnv-somatic-enable-het-calling true \
      --cnv-enable-ref-calls false \
      --enable-variant-annotation true --variant-annotation-assembly GRCh38 --variant-annotation-data /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen_align_inputs/hg38/nirvana_annotation_data \
      --output-format CRAM \
      --intermediate-results-dir /staging/intermediate-results-dir \
      --output-directory $4 \
      --output-file-prefix $(basename $4)
