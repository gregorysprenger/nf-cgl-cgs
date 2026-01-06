#!/usr/bin/env python3

import argparse
import csv
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, cast

import pandas as pd

__version__ = "1.0.0"

TRANS_TABLE = str.maketrans("ATCG", "TAGC")


def parse_args() -> argparse.Namespace:
    """Get command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rundir", required=True, type=Path, help="Path to the Illumina run folder")
    parser.add_argument("-s", "--samplesheet", required=True, type=Path, help="Path to the samplesheet")
    parser.add_argument(
        "-c",
        "--checkindexes",
        action="store_true",
        default=False,
        help="Reverse complement indexes according to the RunInfo.",
    )
    parser.add_argument("-v", "--version", action="version", version="%(prog)s: " + __version__)
    args = parser.parse_args()

    if not args.rundir.exists():
        parser.error(f"The directory {args.rundir} does not exist.")

    if not args.samplesheet.exists():
        parser.error(f"The file {args.samplesheet} does not exist.")

    return args


def reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    return seq.translate(TRANS_TABLE)[::-1]


def get_text_from_xml(element: ET.Element, tag: str) -> str:
    """Safely extract text from an XML element."""
    found = element.find(tag)
    if found is None or found.text is None:
        raise ValueError(f"Tag '{tag}' not found or empty in XML.")
    return found.text.strip()


def _parse_novaseq_x_plus(root: ET.Element, run_dir: Path, run_info: Dict[str, Any]) -> None:
    """Parse NovaSeq X Plus specific parameters."""
    planned_reads = root.find(".//PlannedReads")
    if planned_reads is not None:
        for el in planned_reads.findall("Read"):
            read_name = el.attrib.get("ReadName", "")
            cycles = el.attrib.get("Cycles", "0")
            run_info[f"{read_name}Cycles"] = int(cycles)

    run_info["FlowCellType"] = get_text_from_xml(root, "FlowCellType")
    run_info["InstrumentType"] = get_text_from_xml(root, "InstrumentType")
    run_info["Instrument"] = get_text_from_xml(root, "InstrumentSerialNumber")
    run_info["Side"] = get_text_from_xml(root, "Side")

    # Find the serial number of the flowcell
    consumable_info = root.find("ConsumableInfo")
    if consumable_info is not None:
        for el in consumable_info.findall("ConsumableInfo"):
            el_type = el.find("Type")
            if el_type is not None and el_type.text:
                if el_type.text == "FlowCell":
                    run_info["FlowCellType"] = get_text_from_xml(el, "Mode")
                    run_info["Flowcell"] = get_text_from_xml(el, "SerialNumber")
                    run_info["FlowCellLotNumber"] = get_text_from_xml(el, "LotNumber")
                elif el_type.text == "Reagent":
                    run_info["ReagentLotNumber"] = get_text_from_xml(el, "LotNumber")

    # Parse RunInfo.xml for index orientation
    run_info_path = run_dir / "RunInfo.xml"
    if not run_info_path.exists():
        raise ValueError("RunInfo.xml file not found in the specified directory")

    tree = ET.parse(run_info_path)
    ri_root = tree.getroot()
    run_info_reads = ri_root.findall(".//Read")

    run_info["Index1Reverse"] = "N"
    run_info["Index2Reverse"] = "N"

    if len(run_info_reads) > 1 and run_info_reads[1].attrib.get("IsReverseComplement") == "Y":
        run_info["Index1Reverse"] = "Y"
    if len(run_info_reads) > 2 and run_info_reads[2].attrib.get("IsReverseComplement") == "Y":
        run_info["Index2Reverse"] = "Y"


def _parse_legacy_novaseq(root: ET.Element, run_info: Dict[str, Any]) -> None:
    """Parse legacy NovaSeq 6000 specific parameters."""
    run_info["Read1Cycles"] = int(get_text_from_xml(root, ".//Read1NumberOfCycles"))
    run_info["Read2Cycles"] = int(get_text_from_xml(root, ".//Read2NumberOfCycles"))
    run_info["Index1Cycles"] = int(get_text_from_xml(root, ".//IndexRead1NumberOfCycles"))
    run_info["Index2Cycles"] = int(get_text_from_xml(root, ".//IndexRead2NumberOfCycles"))
    run_info["Flowcell"] = get_text_from_xml(root, ".//FlowCellSerialBarcode")

    run_info["FlowCellType"] = "UNKNOWN"
    run_info["InstrumentType"] = "UNKNOWN"
    run_info["Instrument"] = "UNKNOWN"
    run_info["Side"] = "UNKNOWN"
    run_info["Index1Reverse"] = "N"
    run_info["Index2Reverse"] = "Y"


def parse_run_info(run_dir: Path) -> Dict[str, Any]:
    """Parse RunInfo.xml and RunParameters.xml files to extract run information."""
    run_params_path = run_dir / "RunParameters.xml"
    if not run_params_path.exists():
        raise ValueError("RunParameters.xml file not found in the specified directory")

    tree = ET.parse(run_params_path)
    root = tree.getroot()
    run_info: Dict[str, Any] = {}
    run_info["RunID"] = get_text_from_xml(root, "RunId")

    # Check for NovaSeq Xplus run parameters
    if root.find(".//PlannedReads") is not None:
        _parse_novaseq_x_plus(root, run_dir, run_info)
    else:
        _parse_legacy_novaseq(root, run_info)

    return run_info


def read_samplesheet(file_path: Path) -> pd.DataFrame:
    """Read samplesheet from either xlsx or csv file."""
    if file_path.suffix == ".xlsx":
        return pd.read_excel(file_path, header=0)
    return pd.read_csv(file_path, header=0)


def process_samplesheet(df: pd.DataFrame, run_info: Dict[str, Any], check_indexes: bool) -> pd.DataFrame:
    """Process the samplesheet DataFrame."""
    df.rename(columns={"Content_Desc": "Sample_ID"}, inplace=True)

    split_df = df["Index"].astype(str).str.split("-", n=1, expand=True)
    df["Index"] = split_df[0]
    df["Index2"] = split_df[1] if split_df.shape[1] > 1 else ""

    df[["Index", "Index2"]] = df[["Index", "Index2"]].fillna("")

    if check_indexes:
        if run_info["Index1Reverse"] == "Y":
            df["Index"] = df["Index"].str.translate(TRANS_TABLE).str[::-1]

        if run_info["Index2Reverse"] == "Y":
            df["Index2"] = df["Index2"].str.translate(TRANS_TABLE).str[::-1]

    return df


def generate_cycle_string(run_info: Dict[str, Any]) -> str:
    """Generate the OverrideCycles string for BCLConvert."""
    if run_info["Index1Cycles"] < 19:
        print(f"Number of cycles needs to be >=19, its {run_info['Index1Cycles']}", file=sys.stderr)
        sys.exit(1)

    cycle_str = f"Y{run_info['Read1Cycles']};I10U9"
    if run_info["Index1Cycles"] > 19:
        cycle_str += f"N{run_info['Index1Cycles'] - 19}"

    cycle_str += ";I10"
    if run_info["Index2Cycles"] > 10:
        cycle_str += f"N{run_info['Index2Cycles'] - 10}"

    cycle_str += f";Y{run_info['Read2Cycles']}"
    return cycle_str


def prepare_demux_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare the DataFrame for the demux samplesheet."""
    df = df.assign(Lane=df["Lane"].astype(str).str.split(",")).explode("Lane")
    df["Lane"] = df["Lane"].astype(int)

    output_cols = ["Lane", "Sample_ID", "Index", "Index2"]
    df_output = df[output_cols].copy()

    if not df_output.empty:
        df_output = cast(pd.DataFrame, cast(Any, df_output).sort_values(by=output_cols))

    return cast(pd.DataFrame, df_output)


def write_demux_sheet(df: pd.DataFrame, run_info: Dict[str, Any], cycle_str: str) -> None:
    """Write the BCLConvert demux samplesheet."""
    output_filename = f"{run_info['RunID']}.demux_samplesheet.csv"
    with open(output_filename, "w") as outfile:
        outfile.write("[Header]\nFileFormatVersion,2\n\n")
        outfile.write(
            f"[BCLConvert_Settings]\nAdapterBehavior,trim\n"
            f"AdapterRead1,AGATCGGAAGAGCACACGTCTGAAC\n"
            f"AdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGA\n"
            f"OverrideCycles,{cycle_str}\n\n"
        )
        outfile.write("[BCLConvert_Data]\n")
        df.to_csv(outfile, mode="a", header=True, index=False)


def write_run_info(run_info: Dict[str, Any]) -> None:
    """Write the runinfo dict to a CSV file."""
    run_info_filename = f"{run_info['Flowcell']}.runinfo.csv"
    with open(run_info_filename, "w", newline="") as cfile:
        writer = csv.writer(cfile)
        keys = sorted(run_info.keys())
        writer.writerow(keys)
        writer.writerow([run_info[key] for key in keys])


def main():
    """Main entry point for the script."""
    args = parse_args()
    run_info = parse_run_info(args.rundir)

    df = read_samplesheet(args.samplesheet)
    df = process_samplesheet(df, run_info, args.checkindexes)

    cycle_str = generate_cycle_string(run_info)

    df_output = prepare_demux_sheet(df)

    write_demux_sheet(df_output, run_info, cycle_str)
    write_run_info(run_info)


if __name__ == "__main__":
    main()
