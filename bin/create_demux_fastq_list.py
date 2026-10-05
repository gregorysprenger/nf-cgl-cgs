#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import sys

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["RGID", "RGSM", "RGLB", "Lane", "Read1File", "Read2File"]


class FastqListError(Exception):
    """Raised when an input fastq list or argument is invalid."""


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Combine DRAGEN demultiplex fastq lists, keeping the FastQ paths in the work directory."
    )
    parser.add_argument("-i", "--fastq_lists", nargs="+", required=True, help="Fastq lists from DRAGEN demultiplex.")
    parser.add_argument("-o", "--outdir", default=".", help="Directory to save the combined 'fastq_list.csv'.")

    return parser.parse_args()


def check_input_file(path: str) -> None:
    """Verify an input file, or the target of a symlinked input file, exists and can be read.

    Args:
        path: Path to the input file.

    Raises:
        FastqListError: If the file is missing, a broken symlink, not a regular file, unreadable, or empty.
    """
    if os.path.islink(path) and not os.path.exists(path):
        raise FastqListError(f"Input file '{path}' is a broken symlink to '{os.path.realpath(path)}'.")
    if not os.path.exists(path):
        raise FastqListError(f"Input file '{path}' does not exist.")
    if not os.path.isfile(path):
        raise FastqListError(f"Input file '{path}' is not a regular file.")
    if not os.access(path, os.R_OK):
        raise FastqListError(f"Input file '{path}' is not readable.")
    if os.path.getsize(path) == 0:
        raise FastqListError(f"Input file '{path}' is empty.")


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


def read_fastq_list(path: str) -> tuple[list[str], list[dict[str, str]]]:
    """Read and validate a DRAGEN demultiplex fastq list.

    Args:
        path: Path to the fastq list.

    Returns:
        Column names and rows of the fastq list.

    Raises:
        FastqListError: If the file is inaccessible, is missing required columns, has malformed rows,
        or has FastQ paths that are not absolute.
    """
    check_input_file(path)

    with open(path, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        columns = [c.strip() for c in reader.fieldnames or []]
        reader.fieldnames = columns

        missing = [c for c in REQUIRED_COLUMNS if c not in columns]
        if missing:
            raise FastqListError(f"Fastq list '{path}' is missing required columns: {', '.join(missing)}.")

        rows = []
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise FastqListError(f"Fastq list '{path}' line {line_number} has more fields than the header.")
            if None in row.values():
                raise FastqListError(f"Fastq list '{path}' line {line_number} has fewer fields than the header.")

            row = {c: v.strip() for c, v in row.items()}
            empty = [c for c in REQUIRED_COLUMNS if not row[c]]
            if empty:
                raise FastqListError(
                    f"Fastq list '{path}' line {line_number} has empty values for: {', '.join(empty)}."
                )

            # Paths must point at the FastQ files in the DRAGEN demultiplex work directory
            for column in ("Read1File", "Read2File"):
                if not os.path.isabs(row[column]):
                    raise FastqListError(
                        f"Fastq list '{path}' line {line_number} {column} '{row[column]}' is not an absolute path."
                    )

            rows.append(row)

    if not rows:
        logger.warning("Fastq list '%s' has no rows.", path)

    return columns, rows


def combine_fastq_lists(fastq_lists: list[str]) -> tuple[list[str], list[dict[str, str]]]:
    """Combine fastq lists, keeping their FastQ paths.

    Args:
        fastq_lists: Paths to DRAGEN demultiplex fastq lists.

    Returns:
        Combined column names and rows, sorted for stable output.

    Raises:
        FastqListError: If any fastq list is invalid, or no fastq list has rows.
    """
    columns = []
    rows = []
    for fastq_list in fastq_lists:
        list_columns, list_rows = read_fastq_list(fastq_list)
        columns += [c for c in list_columns if c not in columns]
        rows += list_rows

    if not rows:
        raise FastqListError("No FastQ rows found in any input fastq list.")

    seen = set()
    for row in rows:
        for read in (row["Read1File"], row["Read2File"]):
            if read in seen:
                logger.warning("FastQ file '%s' appears more than once in the combined fastq list.", read)
            seen.add(read)

    rows.sort(key=lambda row: [row.get(c) or "" for c in columns])

    return columns, rows


def write_fastq_list(path: str, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Write a fastq list, replacing any existing file only once writing succeeds.

    Args:
        path: Path to save the fastq list.
        columns: Column names.
        rows: Fastq list rows.
    """
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns, restval="", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)


def main() -> None:
    """Combine demultiplex fastq lists."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    args = parse_args()

    output = os.path.join(args.outdir, "fastq_list.csv")

    try:
        check_output_file(output)
        columns, rows = combine_fastq_lists(args.fastq_lists)
        write_fastq_list(output, columns, rows)
    except (FastqListError, OSError, csv.Error, UnicodeDecodeError) as error:
        logger.error(error)
        sys.exit(1)

    logger.info("Wrote %d rows from %d fastq lists to '%s'.", len(rows), len(args.fastq_lists), output)


if __name__ == "__main__":
    main()
