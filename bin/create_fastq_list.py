#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["RGID", "RGSM", "RGLB", "Lane", "Read1File", "Read2File"]


class FastqListError(Exception):
    """Raised when the input fastq list rows or arguments are invalid."""


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Create a per-sample DRAGEN fastq list with work directory relative FastQ paths."
    )
    parser.add_argument(
        "-r",
        "--rows",
        nargs="+",
        required=True,
        help="Fastq list CSV lines for one sample, header first, in the same order as the staged FastQ files.",
    )
    parser.add_argument("-o", "--output", required=True, help="Path to save the fastq list.")

    return parser.parse_args()


def check_output_file(path: str) -> None:
    """Verify the output file can be written.

    Args:
        path: Path to the output file.

    Raises:
        FastqListError: If the output directory is missing or not writable, or the output path is a directory.
    """
    outdir = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(outdir):
        raise FastqListError(f"Output directory '{outdir}' does not exist.")
    if not os.access(outdir, os.W_OK):
        raise FastqListError(f"Output directory '{outdir}' is not writable.")
    if os.path.isdir(path):
        raise FastqListError(f"Output path '{path}' is a directory.")


def read_rows(lines: list[str]) -> tuple[list[str], list[dict[str, str]]]:
    """Parse and validate fastq list CSV lines.

    FastQ paths are not checked on disk: they are not staged into this task, and INPUT_CHECK
    already verifies they exist and meet the minimum size.

    Args:
        lines: Fastq list CSV lines, header first.

    Returns:
        Column names and rows.

    Raises:
        FastqListError: If required columns are missing, rows are malformed, or FastQ paths are invalid or repeated.
    """
    reader = csv.DictReader(line.lstrip("﻿") if index == 0 else line for index, line in enumerate(lines))
    columns = [c.strip() for c in reader.fieldnames or []]
    reader.fieldnames = columns

    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise FastqListError(f"Fastq list rows are missing required columns: {', '.join(missing)}.")

    rows = []
    seen_reads = set()
    for row_number, row in enumerate(reader, start=1):
        if None in row:
            raise FastqListError(f"Row {row_number} has more fields than the header.")
        if None in row.values():
            raise FastqListError(f"Row {row_number} has fewer fields than the header.")

        row = {c: v.strip() for c, v in row.items()}
        empty = [c for c in REQUIRED_COLUMNS if not row[c]]
        if empty:
            raise FastqListError(f"Row {row_number} has empty values for: {', '.join(empty)}.")

        for column in ("Read1File", "Read2File"):
            if not os.path.basename(row[column]):
                raise FastqListError(f"Row {row_number} {column} '{row[column]}' does not end with a file name.")
        if row["Read1File"] == row["Read2File"]:
            raise FastqListError(f"Row {row_number} uses the same FastQ file for Read1File and Read2File.")

        for read in (row["Read1File"], row["Read2File"]):
            if read in seen_reads:
                raise FastqListError(f"FastQ file '{read}' appears in more than one row.")
            seen_reads.add(read)

        rows.append(row)

    if not rows:
        raise FastqListError("No FastQ rows provided.")

    return columns, rows


def create_fastq_list(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Rewrite FastQ paths to their staged location and disambiguate colliding RGIDs.

    Args:
        rows: Fastq list rows, in the same order as the staged FastQ files.

    Returns:
        Updated fastq list rows.
    """
    # Source directories per RGID, in first-seen order
    dirs_by_rgid = defaultdict(list)
    for row in rows:
        source_dir = os.path.dirname(row["Read1File"])
        if source_dir not in dirs_by_rgid[row["RGID"]]:
            dirs_by_rgid[row["RGID"]].append(source_dir)

    updated_rows = []
    for index, row in enumerate(rows):
        row = dict(row)
        rgid = row["RGID"]

        # Only disambiguate RGIDs that collide across source directories (e.g. flowcells)
        dirs = dirs_by_rgid[rgid]
        if len(dirs) > 1:
            dir_names = [os.path.basename(d) for d in dirs]
            dir_index = dirs.index(os.path.dirname(row["Read1File"]))
            row["RGID"] = (
                f"{dir_names[dir_index]}.{rgid}" if len(set(dir_names)) == len(dirs) else f"{rgid}.{dir_index + 1}"
            )

        # Index must match the 'fastq_files/*/*' stageAs pattern in DRAGEN_ALIGN
        row["Read1File"] = f"fastq_files/{(2 * index) + 1}/{os.path.basename(row['Read1File'])}"
        row["Read2File"] = f"fastq_files/{(2 * index) + 2}/{os.path.basename(row['Read2File'])}"

        updated_rows.append(row)

    read_groups = [(row["RGID"], row["Lane"]) for row in updated_rows]
    duplicates = sorted({rg for rg in read_groups if read_groups.count(rg) > 1})
    if duplicates:
        logger.warning("(RGID, Lane) pairs shared by more than one row: %s", duplicates)

    return updated_rows


def write_fastq_list(path: str, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Write a fastq list, replacing any existing file only once writing succeeds.

    Args:
        path: Path to save the fastq list.
        columns: Column names.
        rows: Fastq list rows.
    """
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)


def main() -> None:
    """Create a per-sample fastq list."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    args = parse_args()

    try:
        check_output_file(args.output)
        columns, rows = read_rows(args.rows)
        write_fastq_list(args.output, columns, create_fastq_list(rows))
    except (FastqListError, OSError, csv.Error) as error:
        logger.error(error)
        sys.exit(1)

    logger.info("Wrote %d rows to '%s'.", len(rows), args.output)


if __name__ == "__main__":
    main()
