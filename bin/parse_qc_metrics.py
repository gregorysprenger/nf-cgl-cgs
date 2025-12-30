#!/usr/bin/env python3

import argparse
import datetime
import glob
import os
from functools import reduce
from typing import Optional

import pandas as pd

METRIC_CONFIGS = {
    "mapping": {
        "suffix": ".mapping_metrics.csv",
        "header": "MAPPING/ALIGNING SUMMARY",
        "metrics": {
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
        },
    },
    "wgs": {
        "suffix": ".wgs_coverage_metrics.csv",
        "header": "COVERAGE SUMMARY",
        "metrics": {
            "Average alignment coverage over genome": 3,
            "Average autosomal coverage over genome": 3,
            "PCT of genome with coverage [  20x: inf)": 3,
            "PCT of genome with coverage [  10x: inf)": 3,
            "PCT Aligned reads in genome": 4,
            "Uniformity of coverage (PCT > 0.2*mean) over genome": 3,
        },
    },
    "qc_region": {
        "suffix": ".qc-coverage-region-1_coverage_metrics.csv",
        "header": "COVERAGE SUMMARY",
        "metrics": {
            "Average alignment coverage over QC coverage region": 3,
            "Average autosomal coverage over QC coverage region": 3,
            "PCT of QC coverage region with coverage [  20x: inf)": 3,
            "PCT of QC coverage region with coverage [  10x: inf)": 3,
            "Uniformity of coverage (PCT > 0.2*mean) over QC coverage region": 3,
        },
    },
    "vc": {
        "suffix": ".vc_metrics.csv",
        "header": "CALLER POSTFILTER",
        "metrics": {
            "Het/Hom ratio": 3,
            "Ti/Tv ratio": 3,
            "Percent Autosome Callability": 3,
        },
    },
    "cnv": {
        "suffix": ".cnv_metrics.csv",
        "header": "",
        "metrics": {"SEX GENOTYPER": 3, "Coverage uniformity": 3},
    },
}


def parseArgs() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(description="Find, parse, and create summary QC metric files.", add_help=False)

    parser.add_argument("-h", "--help", action="help", help="Show usage information and exit.")
    parser.add_argument(
        "-m",
        "--mgi_worksheet",
        nargs="*",
        help="Path to MGI worksheet that contains sequencing information for each sample.",
    )
    parser.add_argument(
        "-i",
        "--inputdir",
        help="Directory to search for QC metric files.",
        required=True,
    )
    parser.add_argument("-o", "--outdir", help="Directory to save summary QC metric files.")
    parser.add_argument("-p", "--prefix", help="Filename prefix to append to output files.")

    return parser.parse_args()


def parse_metrics(files: list[str], metric_dict: dict, line_startswith: str) -> pd.DataFrame:
    """
    Parse list of files for items in dictionary.

    Args:
        files: List of files to search through for metrics.
        metric_dict: Dictionary that maps a value and index of substring to find.
        line_startswith: Only search for metric strings in lines that start with this substring.

    Returns:
        DataFrame that contains specified metrics in metric_dict for all specified files.
    """
    data_list = []

    search_map = {}
    for key, idx in metric_dict.items():
        if key.startswith("PCT "):
            search_map.setdefault(key[4:], []).append((key, idx))
        else:
            search_map.setdefault(key, []).append((key, idx))

    for file in files:
        data_dict = {}
        data_dict["SAMPLE ID"] = os.path.basename(file).split(".")[0]

        try:
            with open(file, "r") as f:
                for line in f:
                    if line_startswith and not line.startswith(line_startswith) and line_startswith not in line:
                        continue

                    parts = line.strip().split(",")
                    for search_key, metrics in search_map.items():
                        if any(part.strip() == search_key or part.strip() == f"PCT {search_key}" for part in parts):
                            for metric_name, col_idx in metrics:
                                if col_idx < len(parts):
                                    data_dict[metric_name] = parts[col_idx]
                            break
        except (OSError, IOError):
            continue

        data_list.append(data_dict)

    return pd.DataFrame(data_list) if data_list else pd.DataFrame(columns=["SAMPLE ID"])


def save_mgi_metrics(
    mgi_worksheet: pd.DataFrame,
    qc_dfs: dict[str, pd.DataFrame],
    filename_prefix: str,
    outdir: str,
) -> None:
    """
    Save required QC metrics for MGI.

    Args:
        mgi_worksheet: QC metrics sheet.
        qc_dfs: Dictionary containing parsed QC DataFrames.
        filename_prefix: Prefix for output filenames.
        outdir: Output directory to save file.
    """
    df = mgi_worksheet.copy()
    for key in ["mapping", "wgs", "qc_region"]:
        if not qc_dfs[key].empty:
            df = pd.merge(df, qc_dfs[key], on="SAMPLE ID", how="outer")

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
        "Average autosomal coverage over genome": "AVG_AUTOSOMAL_GENOME_COVERAGE",
        "PCT of genome with coverage [  20x: inf)": "PCT_GENOME_20x",
        "PCT of genome with coverage [  10x: inf)": "PCT_GENOME_10x",
        "Average autosomal coverage over QC coverage region": "AVG_AUTOSOMAL_EXOME_COVERAGE",
        "PCT of QC coverage region with coverage [  20x: inf)": "PCT_EXOME_20x",
        "Uniformity of coverage (PCT > 0.2*mean) over genome": "PCT_UNIFORM_COVERAGE",
        "PCT Aligned reads in genome": "PCT_GENOME_ALIGNED_READS",
    }

    df.rename(columns=columns_to_rename, inplace=True)

    cols = [
        "ACCESSION NUMBER",
        "RUN ID",
        "SAMPLE ID",
        "Total DNA yield (ng)",
        "260/280",
        "Library Input (ng)",
        "Capture Input (ng)",
    ] + list(columns_to_rename.values())

    df = df[[c for c in cols if c in df.columns]]
    df.to_excel(
        f"{outdir}/{filename_prefix}_MGI_QC.xlsx",
        index=False,
        sheet_name="MGI QC metrics",
    )


def save_all_metrics(all_qc_dataframes: list[pd.DataFrame], filename_prefix: str, outdir: str) -> None:
    """
    Save all QC metrics.

    Args:
        all_qc_dataframes: List of QC metric DataFrames.
        filename_prefix: Prefix for output filenames.
        outdir: Output directory to save file.
    """

    def merge_dfs(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        """Merge two DataFrames on the SAMPLE ID column.

        Args:
            left: First DataFrame to merge.
            right: Second DataFrame to merge.

        Returns:
            Merged DataFrame.
        """
        return pd.merge(left, right, on="SAMPLE ID", how="outer") if not right.empty else left

    all_qc_metrics = reduce(
        merge_dfs,
        all_qc_dataframes,
    )
    all_qc_metrics.to_excel(
        f"{outdir}/{filename_prefix}_QC.xlsx",
        index=False,
        sheet_name="QC metrics",
        engine="openpyxl",
    )


def save_genoox_metrics(
    mgi_worksheet: pd.DataFrame,
    mapping_metrics: pd.DataFrame,
    filename_prefix: str,
    outdir: str,
) -> None:
    """
    Create Excel workbook that contains the following sheets: QC Metrics - qPCR.

    Args:
        mgi_worksheet: QC metrics sheet from the MGI worksheet input.
        mapping_metrics: Metrics pulled from '*.mapping_metrics.csv' files - ONLY SAMPLE_ID column is used.
        filename_prefix: Prefix for output filenames.
        outdir: Output directory to save file.
    """
    required_columns = [
        "ACCESSION NUMBER",
        "RUN ID",
        "SAMPLE ID",
        "Total DNA yield (ng)",
        "260/280",
        "Library Input (ng)",
    ]
    cleaned_mgi_worksheet = mgi_worksheet[[c for c in required_columns if c in mgi_worksheet.columns]]

    if cleaned_mgi_worksheet["SAMPLE ID"].isnull().all() or cleaned_mgi_worksheet.empty:
        cleaned_mgi_worksheet = cleaned_mgi_worksheet.merge(mapping_metrics, on="SAMPLE ID", how="right")
        cleaned_mgi_worksheet = cleaned_mgi_worksheet[required_columns]

    is_genoox_sample = cleaned_mgi_worksheet["SAMPLE ID"].str.startswith("G", na=False)
    cleaned_mgi_worksheet = cleaned_mgi_worksheet[is_genoox_sample]

    if cleaned_mgi_worksheet.empty:
        return

    cleaned_mgi_worksheet.to_excel(
        f"{outdir}/{filename_prefix}_Genoox.xlsx",
        sheet_name="QC Metrics - qPCR",
        index=False,
    )


def read_file_to_dataframe(file: Optional[str]) -> pd.DataFrame:
    """
    Read input file to DataFrame.

    Args:
        file: Input file to read.

    Returns:
        DataFrame containing data from input file.
    """
    if not file:
        df = pd.DataFrame()
    else:
        try:
            if file.endswith(".tsv"):
                df = pd.read_csv(file, sep="\t")
            elif file.endswith(".csv"):
                df = pd.read_csv(file, sep=",")
            elif file.endswith(".xlsx"):
                df = pd.read_excel(file, sheet_name="QC Metrics")
            else:
                df = pd.DataFrame()
        except (ValueError, FileNotFoundError):
            return pd.DataFrame()

    if "Content_Desc" in df:
        if "SAMPLE ID" not in df:
            df.rename(columns={"Content_Desc": "SAMPLE ID"}, inplace=True)
        elif df["SAMPLE ID"].fillna("").eq("").all():
            df["SAMPLE ID"] = df["Content_Desc"].copy()

    cols = [
        "ACCESSION NUMBER",
        "RUN ID",
        "SAMPLE ID",
        "Total DNA yield (ng)",
        "260/280",
        "Library Input (ng)",
    ]

    df = df.reindex(columns=cols)

    obj_cols = df.select_dtypes(include=["object"]).columns
    if not obj_cols.empty:
        df[obj_cols] = df[obj_cols].apply(lambda x: x.str.strip())

    return df


def main() -> None:
    """
    Parse QC metrics for all files and save to Excel workbooks.
    """
    args = parseArgs()

    inputdir = os.path.abspath(args.inputdir)
    if args.mgi_worksheet:
        mgi_worksheet = pd.concat([read_file_to_dataframe(f) for f in args.mgi_worksheet], ignore_index=True)
    else:
        mgi_worksheet = read_file_to_dataframe(None)

    outdir = os.path.abspath(args.outdir) if args.outdir else os.getcwd()
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)

    if args.prefix:
        filename_prefix = args.prefix
    else:
        timestamp = datetime.date.today().strftime("%Y%m%d")
        filename_prefix = f"{timestamp}_CGS"

    metric_files = glob.glob(f"{inputdir}/**/*metrics.csv", recursive=True)

    files_by_type = {k: [] for k in METRIC_CONFIGS}

    for f in metric_files:
        f_strip = f.strip()
        for key, config in METRIC_CONFIGS.items():
            if f_strip.endswith(config["suffix"]):
                files_by_type[key].append(f_strip)
                break

    qc_dfs = {}
    for key, config in METRIC_CONFIGS.items():
        df = parse_metrics(files_by_type[key], config["metrics"], config["header"])
        if key == "mapping" and "Total bases" in df.columns:
            df.insert(3, "Total giga bases", round(df["Total bases"].astype(float) / 1e9, 2))
        qc_dfs[key] = df

    # Create output files
    ## MGI metrics
    save_mgi_metrics(mgi_worksheet, qc_dfs, filename_prefix, outdir)

    ## Genoox metrics
    save_genoox_metrics(
        mgi_worksheet,
        qc_dfs["mapping"],
        filename_prefix,
        outdir,
    )

    ## All metrics
    all_qc_dataframes = [mgi_worksheet] + [qc_dfs[k] for k in ["mapping", "wgs", "qc_region", "vc", "cnv"]]
    save_all_metrics(all_qc_dataframes, f"{filename_prefix}_All", outdir)


if __name__ == "__main__":
    main()
