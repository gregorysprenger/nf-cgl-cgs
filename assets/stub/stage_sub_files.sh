#!/usr/bin/env bash

PWD=$(realpath .)

sed -e 's!REPLACEPATH!'$PWD'!g' fastq_list_wgs_mastersheet.csv.orig > fastq_list_wgs_mastersheet.csv
sed -e 's!REPLACEPATH!'$PWD'!g' reads_wgs_mastersheet.csv.orig > reads_wgs_mastersheet.csv
sed -e 's!REPLACEPATH!'$PWD'!g' cram_wgs_test_mastersheet.csv.orig > cram_wgs_test_mastersheet.csv
sed -e 's!REPLACEPATH!'$PWD'!g' demux_fastq/fastq_list.csv.orig > fastq_list.csv
