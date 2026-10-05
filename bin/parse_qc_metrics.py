#!/usr/bin/env python3

import argparse
import datetime as dt
import glob
import logging
import os
from functools import reduce
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

WORKSHEET_COLUMNS = [
    "ACCESSION NUMBER",
    "RUN ID",
    "SAMPLE ID",
    "Total DNA yield (ng)",
    "260/280",
    "Library Input (ng)",
]

MGI_COLUMN_RENAMES = {
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

MGI_QC_COLUMNS = WORKSHEET_COLUMNS + list(MGI_COLUMN_RENAMES.values())

# Number of '_' separated parts in a run ID that ends with a flowcell
RUN_ID_PARTS = 4

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


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(description="Find, parse, and create summary QC metric files.")
    parser.add_argument(
        "-m",
        "--mgi_worksheet",
        nargs="*",
        default=[],
        help="Path to MGI worksheet that contains sequencing information for each sample.",
    )
    parser.add_argument("-i", "--inputdir", required=True, help="Directory to search for QC metric files.")
    parser.add_argument("-o", "--outdir", help="Directory to save summary QC metric files.")
    parser.add_argument("-p", "--prefix", help="Filename prefix to append to output files.")

    return parser.parse_args()


def parse_metrics(files: list[str], metric_dict: dict[str, int], section_header: str) -> pd.DataFrame:
    """Parse DRAGEN metric files into one row per sample.

    Args:
        files: Metric files to parse. The SAMPLE ID is taken from the filename.
        metric_dict: Metric name to the column index holding its value.
        section_header: Only search for metrics in lines containing this substring.

    Returns:
        DataFrame with a SAMPLE ID column and one column per metric found.
    """
    metrics_by_name = {}
    for name, col_idx in metric_dict.items():
        metrics_by_name.setdefault(name.removeprefix("PCT "), []).append((name, col_idx))

    rows = []
    for file in files:
        row = {"SAMPLE ID": os.path.basename(file).split(".")[0]}

        try:
            with open(file) as f:
                for line in f:
                    if section_header not in line:
                        continue

                    parts = [part.strip() for part in line.split(",")]
                    metrics = next(
                        (
                            metrics_by_name[key]
                            for key in (p.removeprefix("PCT ") for p in parts)
                            if key in metrics_by_name
                        ),
                        [],
                    )
                    row.update({name: parts[col_idx] for name, col_idx in metrics if col_idx < len(parts)})
        except OSError as error:
            logger.warning("Could not read %s: %s", file, error)
            continue

        rows.append(row)

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["SAMPLE ID"])


def collect_qc_metrics(inputdir: str) -> dict[str, pd.DataFrame]:
    """Find and parse every DRAGEN metric file under a directory.

    Args:
        inputdir: Directory to search recursively for QC metric files.

    Returns:
        Mapping of METRIC_CONFIGS key to the DataFrame parsed from those files.
    """
    qc_dfs = {
        key: parse_metrics(
            sorted(glob.glob(f"{inputdir}/**/*{config['suffix']}", recursive=True)),
            config["metrics"],
            config["header"],
        )
        for key, config in METRIC_CONFIGS.items()
    }

    mapping_metrics = qc_dfs["mapping"]
    if "Total bases" in mapping_metrics:
        mapping_metrics.insert(
            min(3, len(mapping_metrics.columns)),
            "Total giga bases",
            round(pd.to_numeric(mapping_metrics["Total bases"], errors="coerce") / 1e9, 2),
        )

    return qc_dfs


def read_worksheet(file: str) -> pd.DataFrame:
    """Read an MGI worksheet, keeping only the worksheet columns.

    Args:
        file: Worksheet to read (.csv, .tsv, or .xlsx).

    Returns:
        DataFrame with WORKSHEET_COLUMNS and whitespace-stripped text values.
    """
    try:
        if file.endswith(".xlsx"):
            df = pd.read_excel(file, sheet_name="QC Metrics")
        elif file.endswith((".csv", ".tsv")):
            df = pd.read_csv(file, sep="\t" if file.endswith(".tsv") else ",")
        else:
            logger.warning("Unsupported worksheet format: %s", file)
            df = pd.DataFrame()
    except (ValueError, FileNotFoundError) as error:
        logger.warning("Could not read %s: %s", file, error)
        df = pd.DataFrame()

    sample_ids = df.get("SAMPLE ID")
    if "Content_Desc" in df and (sample_ids is None or sample_ids.fillna("").eq("").all()):
        df["SAMPLE ID"] = df["Content_Desc"]

    df = df.reindex(columns=WORKSHEET_COLUMNS)
    df["SAMPLE ID"] = df["SAMPLE ID"].astype(object)

    text_cols = df.select_dtypes("object").columns
    df[text_cols] = df[text_cols].apply(lambda col: col.fillna("").astype(str).str.strip())

    return df


def collapse_run_ids(run_ids: pd.Series) -> Optional[str]:
    """Collapse the run IDs of worksheet rows that differ only by RUN ID.

    Args:
        run_ids: Series of run IDs for one sample.

    Returns:
        The run ID if there is only one, the shared run ID without flowcell if several run IDs differ only by
        flowcell, all full run IDs joined by ';' (with a warning) otherwise, or None if all run IDs are empty.
    """
    unique_ids = list(dict.fromkeys(r for r in (str(r).strip() for r in run_ids.dropna()) if r))
    if len(unique_ids) <= 1:
        return unique_ids[0] if unique_ids else None

    # Run IDs are <date>_<instrument>_<run number>_<flowcell>; only strip the flowcell when every ID has one
    prefixes = {r.rsplit("_", 1)[0] for r in unique_ids}
    if len(prefixes) == 1 and all(len(r.split("_")) >= RUN_ID_PARTS for r in unique_ids):
        return prefixes.pop()

    logger.warning("Rows differ only by RUN ID but runs do not match: %s. Keeping all runs.", unique_ids)
    return ";".join(unique_ids)


def load_worksheets(files: list[str]) -> pd.DataFrame:
    """Read and combine MGI worksheets, collapsing rows that differ only by RUN ID.

    Args:
        files: Worksheets to read. May be empty.

    Returns:
        Combined worksheet, where RUN ID is collapsed as described in collapse_run_ids.
    """
    if not files:
        return pd.DataFrame(columns=WORKSHEET_COLUMNS)

    worksheet = pd.concat(map(read_worksheet, files), ignore_index=True)
    key_cols = [c for c in WORKSHEET_COLUMNS if c != "RUN ID"]

    return worksheet.groupby(key_cols, dropna=False, sort=False, as_index=False).agg({"RUN ID": collapse_run_ids})[
        WORKSHEET_COLUMNS
    ]


def align_sample_ids(qc_dfs: dict[str, pd.DataFrame], worksheet: pd.DataFrame) -> None:
    """Rewrite QC SAMPLE IDs in place to match their MGI worksheet spelling.

    Args:
        qc_dfs: Parsed QC DataFrames. Modified in place.
        worksheet: Worksheet supplying the canonical SAMPLE ID spellings.
    """
    worksheet_ids = sorted({str(w_id).strip() for w_id in worksheet["SAMPLE ID"].dropna()})
    worksheet_ids_upper = {w_id.upper(): w_id for w_id in worksheet_ids}

    remap = {}
    for qc_id in sorted({qc_id for df in qc_dfs.values() for qc_id in df["SAMPLE ID"].dropna()}):
        qc_id_stripped = qc_id.strip()
        qc_id_upper = qc_id_stripped.upper()
        if not qc_id_upper:
            continue

        if qc_id_upper in worksheet_ids_upper:
            matches = [worksheet_ids_upper[qc_id_upper]]
        else:
            n = len(qc_id_stripped)
            matches = [
                w_id
                for w_id in worksheet_ids
                if w_id.upper().startswith(qc_id_upper) and len(w_id) > n and not w_id[n].isalnum()
            ]

        if len(matches) > 1:
            logger.warning("Ambiguous prefix match for %s: %s. Skipping remapping.", qc_id_stripped, matches)
        elif matches and matches[0] != qc_id:
            remap[qc_id] = matches[0]

    if remap:
        logger.info("Remapping SAMPLE IDs: %s", remap)
        for df in qc_dfs.values():
            df["SAMPLE ID"] = df["SAMPLE ID"].replace(remap)


def merge_on_sample_id(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Outer merge DataFrames on SAMPLE ID, skipping empty ones after the first.

    Args:
        dfs: DataFrames to merge.

    Returns:
        Merged DataFrame.
    """
    return reduce(lambda left, right: left if right.empty else left.merge(right, on="SAMPLE ID", how="outer"), dfs)


def genoox_metrics(worksheet: pd.DataFrame, mapping_metrics: pd.DataFrame) -> pd.DataFrame:
    """Select Genoox samples (IDs starting with 'G' or containing 'WCN-') from the worksheet.

    Args:
        worksheet: Combined MGI worksheet.
        mapping_metrics: Mapping metrics, used for SAMPLE IDs when the worksheet has none.

    Returns:
        Worksheet rows for Genoox samples.
    """
    if worksheet["SAMPLE ID"].fillna("").eq("").all():
        worksheet = mapping_metrics[["SAMPLE ID"]].reindex(columns=WORKSHEET_COLUMNS)

    sample_ids = worksheet["SAMPLE ID"]
    return worksheet[sample_ids.str.startswith("G", na=False) | sample_ids.str.contains("WCN-", na=False, regex=False)]


def main() -> None:
    """Parse QC metrics for all files and save to Excel workbooks."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    args = parse_args()

    outdir = os.path.abspath(args.outdir) if args.outdir else os.getcwd()
    os.makedirs(outdir, exist_ok=True)
    prefix = args.prefix or f"{dt.date.today():%Y%m%d}_CGS"

    def write_excel(df: pd.DataFrame, name: str, sheet_name: str) -> None:
        df.to_excel(
            os.path.join(outdir, f"{prefix}_{name}.xlsx"), index=False, sheet_name=sheet_name, engine="openpyxl"
        )

    worksheet = load_worksheets(args.mgi_worksheet)
    qc_dfs = collect_qc_metrics(os.path.abspath(args.inputdir))
    align_sample_ids(qc_dfs, worksheet)

    mgi_qc = merge_on_sample_id([worksheet] + [qc_dfs[key] for key in ["mapping", "wgs", "qc_region"]])
    write_excel(mgi_qc.rename(columns=MGI_COLUMN_RENAMES).filter(items=MGI_QC_COLUMNS), "MGI_QC", "MGI QC metrics")

    genoox = genoox_metrics(worksheet, qc_dfs["mapping"])
    if not genoox.empty:
        write_excel(genoox, "Genoox", "QC Metrics - qPCR")

    all_qc = merge_on_sample_id([worksheet] + [qc_dfs[key] for key in ["mapping", "wgs", "qc_region", "vc", "cnv"]])
    write_excel(all_qc, "All_QC", "QC metrics")


if __name__ == "__main__":
    main()
