#!/usr/bin/env python3

import argparse
import glob
import os
import re
import sys
from functools import reduce

import pandas as pd


def parseArgs():
    """
    Parse input parameters.

    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Find, parse, and create summary QC metric files.", add_help=False
    )

    parser.add_argument(
        "-h", "--help", action="help", help="Show usage information and exit."
    )
    parser.add_argument(
        "-m",
        "--mgi_worksheet",
        help="Path to MGI worksheet that contains sequencing information for each sample.",
    )
    parser.add_argument(
        "-i",
        "--inputdir",
        help="Directory to search for QC metric files.",
        required=True,
    )
    parser.add_argument(
        "-o", "--outdir", help="Directory to save summary QC metric files."
    )
    parser.add_argument(
        "-p", "--prefix", help="Filename prefix to append to output files."
    )

    return parser.parse_args()


def get_output_directory(outdir):
    """
    Get absolute path of output directory if specified. If not specified, set it to the current working directory.

    :param outdir: User specified path to output directory
    :return: Absolute path to output directory
    """
    if not outdir:
        output_dir = os.getcwd()
    else:
        output_dir = os.path.abspath(outdir)

    if not os.path.isdir(output_dir):
        os.mkdir(os.path.abspath(output_dir))

    return output_dir


def get_list_value(lst, sep, index, value):
    """
    Find the first string match in provided list, and then split based on provided separator and index.

    :param lst: Input list of strings to search through
    :param sep: Separator to split text by
    :param index: Index to keep after splitting the text
    :param value: Only look at strings that contain this substring
    :return: Returns identified substring
    """
    return next((s.split(sep)[index] for s in lst if value in s), None)


def get_columns(df, col_list):
    """
    Subset a DataFrame from a list of columns if those columns are present in the DataFrame.

    :param df: DataFrame to subset columns from
    :param col_list: List of column names to subset DataFrame
    :return: DataFrame with a specified subset of columns if they exist
    """
    return df[[col for col in col_list if col in df.columns]].copy()


def parse_mapping_metrics(metric_files):
    """
    Parse metrics out of all files that end with `mapping_metrics.csv`.

    :param metric_files: List of all metric CSV files
    :return: DataFrame containing metrics for each `mapping_metrics.csv` file
    """
    metric_dict = {
        "Total input reads": 3,
        "Total bases": 3,
        "Mapped reads": 3,
        "PCT Mapped reads": 4,
        "Number of unique reads (excl. duplicate marked reads)": 3,
        "PCT Number of unique reads (excl. duplicate marked reads)": 4,
        "Number of duplicate marked reads": 3,
        "PCT Number of duplicate marked reads": 4,
        "Paired reads (itself & mate mapped)": 4,
        "Not properly paired reads (discordant)": 4,
        "PCT Mismatched bases R1": 4,
        "PCT Mismatched bases R2": 4,
        "Q30 bases R1": 4,
        "PCT Q30 bases R1": 4,
        "Q30 bases R2": 4,
        "PCT Q30 bases R2": 4,
        "Insert length: median": 3,
        "Insert length: mean": 3,
        "Estimated sample contamination": 3,
    }

    metrics_files = [
        f.strip() for f in metric_files if f.endswith(".mapping_metrics.csv")
    ]

    df = parse_metrics(metrics_files, metric_dict, "MAPPING/ALIGNING SUMMARY")
    if "Total bases" in df.columns:
        df.insert(
            3,
            "Total giga bases",
            round(df["Total bases"].astype(float) / 1000000000, 2),
        )

    return df


def parse_wgs_coverage_metrics(metric_files):
    """
    Parse metrics out of all files that end with `wgs_coverage_metrics.csv`.

    :param metric_files: List of all metric CSV files
    :return: DataFrame containing metrics for each `wgs_coverage_metrics.csv` file
    """
    metric_dict = {
        "Average alignment coverage over genome": 3,
        "PCT of genome with coverage [  20x: inf)": 3,
        "PCT of genome with coverage [  10x: inf)": 3,
        "PCT Aligned reads in genome": 4,
        "Uniformity of coverage (PCT > 0.2*mean) over genome": 3,
    }

    metrics_files = [
        f.strip() for f in metric_files if f.endswith(".wgs_coverage_metrics.csv")
    ]

    return parse_metrics(metrics_files, metric_dict, "COVERAGE SUMMARY")


def parse_qc_coverage_region_metrics(metric_files):
    """
    Parse metrics out of all files that end with `qc-coverage-region-1_coverage_metrics.csv`.

    :param metric_files: List of all metric CSV files
    :return: DataFrame containing metrics for each `qc-coverage-region-1_coverage_metrics.csv` file
    """
    metric_dict = {
        "Average alignment coverage over QC coverage region": 3,
        "PCT of QC coverage region with coverage [  20x: inf)": 3,
        "PCT of QC coverage region with coverage [  10x: inf)": 3,
        "Uniformity of coverage (PCT > 0.2*mean) over QC coverage region": 3,
    }

    metrics_files = [
        f.strip()
        for f in metric_files
        if f.endswith(".qc-coverage-region-1_coverage_metrics.csv")
    ]

    return parse_metrics(metrics_files, metric_dict, "COVERAGE SUMMARY")


def parse_vc_metrics(metric_files):
    """
    Parse metrics out of all files that end with `vc_metrics.csv`.

    :param metric_files: List of all metric CSV files
    :return: DataFrame containing metrics for each `vc_metrics.csv` file
    """
    metric_dict = {
        "Het/Hom ratio": 3,
        "Ti/Tv ratio": 3,
        "Percent Autosome Callability": 3,
    }

    metrics_files = [f.strip() for f in metric_files if f.endswith(".vc_metrics.csv")]

    return parse_metrics(metrics_files, metric_dict, "CALLER POSTFILTER")


def parse_cnv_metrics(metric_files):
    """
    Parse metrics out of all files that end with `cnv_metrics.csv`.

    :param metric_files: List of all metric CSV files
    :return: DataFrame containing metrics for each `cnv_metrics.csv` file
    """
    metric_dict = {"SEX GENOTYPER": 3, "Coverage uniformity": 3}

    metrics_files = [f.strip() for f in metric_files if f.endswith(".cnv_metrics.csv")]

    return parse_metrics(metrics_files, metric_dict, "")


def parse_metrics(files, metric_dict, line_startswith):
    """
    Parse list of files for items in dictionary.

    :param files: List of files to search through for metrics
    :param metric_dict: Dictionary that maps a value and index of substring to find
    :param line_startswith: Only search for metric strings in lines that start with this substring
    :return: DataFrame that contains specified metrics in metric_dict for all specified files
    """
    dataframe_list = []

    for file in files:
        lines = [
            line.strip()
            for line in open(file)
            if line.startswith(line_startswith) or line_startswith in line
        ]

        data_dict = {}
        data_dict["SAMPLE ID"] = os.path.basename(file).split(".")[0]

        for k, v in metric_dict.items():
            if k.startswith("PCT"):
                search = k.split("PCT ")[1]
            else:
                search = k

            data_dict[k] = get_list_value(lines, ",", v, search)

        dataframe_list.append(pd.DataFrame(data_dict, index=[0]))

    if len(dataframe_list) > 0:
        return pd.concat(dataframe_list)
    else:
        return pd.DataFrame(columns=["SAMPLE ID"])


def save_mgi_metrics(
    mgi_worksheet, mapping_metrics, wgs_coverage_metrics, filename_prefix, outdir
):
    """
    Save required QC metrics for MGI.

    :param mgi_worksheet: QC metrics sheet from the MGI worksheet input
    :param mapping_metrics: Metrics pulled from '*.mapping_metrics.csv' files
    :param wgs_coverage_metrics: Metrics pulled from '*.wgs_coverage_metrics.csv' files
    :param filename_prefix: Prefix for output filenames
    :param outdir: Output directory to save file
    """
    qc_dataframes = [mgi_worksheet, mapping_metrics, wgs_coverage_metrics]
    df = reduce(
        lambda left, right: pd.merge(left, right, on=["SAMPLE ID"], how="outer"),
        qc_dataframes,
    )

    columns_to_rename = {
        "Total input reads": "TOTAL_READS",
        "PCT Number of duplicate marked reads": "PCT_DUPLICATE_READS",
        "PCT Mapped reads": "PCT_MAPPED_READS",
        "Total bases": "TOTAL_BASES",
        "Total giga bases": "TOTAL_GIGA_BASES",
        "PCT Mismatched bases R1": "MISMATCHED_RATE_R1",
        "PCT Mismatched bases R2": "MISMATCHED_RATE_R2",
        "PCT Q30 bases R1": "PCT_Q30_BASES_1",
        "PCT Q30 bases R2": "PCT_Q30_BASES_2",
        "Insert length: mean": "MEAN_INS_SIZE",
        "Average alignment coverage over genome": "AVG_ALIGN_GENOME_COVERAGE",
        "Uniformity of coverage (PCT > 0.2*mean) over genome": "PCT_UNIFORM_COVERAGE",
        "PCT Aligned reads in genome": "PCT_GENOME_ALIGNED_READS",
        "PCT of genome with coverage [  20x: inf)": "PCT_GENOME_20x",
        "PCT of genome with coverage [  10x: inf)": "PCT_GENOME_10x",
    }

    df.rename(columns=columns_to_rename, inplace=True)

    # Output select columns
    df = get_columns(
        df,
        [
            "ACCESSION NUMBER",
            "RUN ID",
            "SAMPLE ID",
            "Total DNA yield (ng)",
            "260/280",
            "Library Input (ng)",
            "Capture Input (ng)",
        ]
        + list(columns_to_rename.values()),
    )
    df.to_excel(
        f"{outdir}/{filename_prefix}_MGI_QC.xlsx",
        index=False,
        sheet_name="MGI QC metrics",
    )


def save_all_metrics(all_qc_dataframes, filename_prefix, outdir):
    """
    Save all QC metrics.

    :param all_qc_dataframes: List of QC metric DataFrames
    :param filename_prefix: Prefix for output filenames
    :param outdir: Output directory to save file
    """
    all_qc_metrics = reduce(
        lambda left, right: pd.merge(left, right, on=["SAMPLE ID"], how="outer"),
        all_qc_dataframes,
    )
    all_qc_metrics.to_excel(
        f"{outdir}/{filename_prefix}_QC.xlsx", index=False, sheet_name="QC metrics"
    )


def save_genoox_metrics(mgi_worksheet, mapping_metrics, filename_prefix, outdir):
    """
    Create Excel workbook that contains the following sheets: QC Metrics - qPCR, Single Sample Stats, and Final Coverage Stats - TCP.

    :param mgi_worksheet: QC metrics sheet from the MGI worksheet input
    :param mapping_metrics: Metrics pulled from '*.mapping_metrics.csv' files
    :param filename_prefix: Prefix for output filenames
    :param outdir: Output directory to save file
    """
    # Use only specified columns from MGI worksheet
    cleaned_mgi_worksheet = get_columns(
        mgi_worksheet,
        [
            "ACCESSION NUMBER",
            "RUN ID",
            "SAMPLE ID",
            "Total DNA yield (ng)",
            "260/280",
            "Library Input (ng)",
        ],
    )

    # Get specified single sample metrics
    single_sample_stats = get_columns(
        mapping_metrics,
        ["SAMPLE ID", "Total bases", "PCT Q30 bases R1", "PCT Q30 bases R2"],
    )
    single_sample_stats.rename(
        lambda c: c
        if any(
            k in c
            for k in {
                "SAMPLE ID": "Library",
                "Total bases": "Total Bases",
                "PCT Q30 bases R1": "Percent Q30 (R1)",
                "PCT Q30 bases R2": "Percent Q30 (R2)",
            }
        )
        else c,
        axis=1,
        inplace=True,
    )

    # Get sample contamination metrics
    final_coverage_stats = get_columns(
        mapping_metrics, ["SAMPLE ID", "Estimated sample contamination"]
    )
    final_coverage_stats.rename(
        columns={
            "SAMPLE ID": "Sample",
            "Estimated sample contamination": "contaminationestimate",
        }
    )

    # Create dict to house DataFrames
    dataframe_dict = {
        "QC Metrics - qPCR": cleaned_mgi_worksheet,
        "Single Sample Stats": single_sample_stats,
        "Final Coverage Stats - TCP": final_coverage_stats,
    }

    # Create workbook and write data
    writer = pd.ExcelWriter(
        f"{outdir}/{filename_prefix}_Genoox.xlsx", engine="xlsxwriter"
    )
    for sheetname, df_data in dataframe_dict.items():
        df_data.to_excel(writer, index=False, sheet_name=sheetname)
    writer.close()


def main():
    """
    Parse QC metrics for all files and save to Excel workbooks.
    """
    args = parseArgs()

    # Check inputs
    inputdir = os.path.abspath(args.inputdir)

    if args.mgi_worksheet:
        mgi_worksheet = pd.read_excel(args.mgi_worksheet, sheet_name="QC Metrics")
    else:
        mgi_worksheet = pd.DataFrame(columns=["SAMPLE ID"])

    outdir = get_output_directory(args.outdir)

    if args.prefix:
        filename_prefix = args.prefix
    else:
        filename_prefix = ""

    # Gather all DRAGEN metric files
    metric_files = [f for f in glob.glob(f"{inputdir}/**/*metrics.csv", recursive=True)]

    mapping_metrics = parse_mapping_metrics(metric_files)
    wgs_coverage_metrics = parse_wgs_coverage_metrics(metric_files)
    qc_coverage_region = parse_qc_coverage_region_metrics(metric_files)
    vc_metrics = parse_vc_metrics(metric_files)
    cnv_metrics = parse_cnv_metrics(metric_files)

    # Create output files
    ## MGI metrics
    save_mgi_metrics(
        mgi_worksheet, mapping_metrics, wgs_coverage_metrics, filename_prefix, outdir
    )

    ## Genoox metrics
    save_genoox_metrics(mgi_worksheet, mapping_metrics, filename_prefix, outdir)

    ## All metrics
    all_qc_dataframes = [
        mgi_worksheet,
        mapping_metrics,
        wgs_coverage_metrics,
        qc_coverage_region,
        vc_metrics,
        cnv_metrics,
    ]
    save_all_metrics(all_qc_dataframes, f"{filename_prefix}_All", outdir)


if __name__ == "__main__":
    main()
