# precompute_mongo_data.py
import argparse
import copy
import csv
import datetime
import json
import sys
import zlib
from collections import OrderedDict
from pathlib import Path

# --- Configuration ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.device"
TSV_INTERFACE_COL = "meta.name"
DISCARD_TSV_FIELDS = {"@collect_time_min", "@exit_time", "@processing_time"}
MONGO_TIME_FIELD = "timestamp"
MONGO_META_FIELD = "meta"


def parse_timestamp(ts_string):
    """Parses timestamp string into a datetime object."""
    try:
        if ts_string.endswith("Z"):
            ts_string = ts_string[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(ts_string)
    except Exception:
        return None


def set_nested_value(doc: dict, path_str: str, value):
    """Sets a value in a nested dictionary from a dot-separated path."""
    keys = path_str.split(".")
    current_level = doc
    for key in keys[:-1]:
        current_level = current_level.setdefault(key, {})
    current_level[keys[-1]] = value


def precompute_main(
    input_tsv_file: Path, output_dir: Path, total_workers: int, total_data_lines: int
):
    if not input_tsv_file.is_file():
        print(f"Error: Input TSV file not found: {input_tsv_file}", file=sys.stderr)
        sys.exit(1)
    if total_workers < 1:
        print(f"Error: Total workers must be at least 1.", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory for precomputed files: {output_dir}")
    print(f"Input TSV: {input_tsv_file}")
    print(f"Total worker files to create: {total_workers}")
    if total_data_lines:
        print(f"Expecting {total_data_lines:,} data lines.")

    header_list = None
    try:
        with open(input_tsv_file, "r", newline="", encoding="utf-8") as f_header:
            reader = csv.reader(f_header, delimiter="\t")
            header_list = next(reader)
        if not header_list:
            print("Error: Could not read header from input file.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error reading header: {e}", file=sys.stderr)
        sys.exit(1)

    # --- Create a template for the metadata object to ensure consistent shape ---
    meta_template = {}
    print("Building metadata template from TSV header...")
    for key in header_list:
        path_to_set = None
        if key == TSV_NODE_COL:
            path_to_set = "device"
        elif key == TSV_INTERFACE_COL:
            path_to_set = "interfaceName"
        elif key.startswith("meta."):
            path_to_set = key.replace("meta.", "", 1)
        elif (
            not key.startswith("values.")
            and key != TSV_TIMESTAMP_COL
            and key not in DISCARD_TSV_FIELDS
        ):
            path_to_set = key

        if path_to_set:
            set_nested_value(meta_template, path_to_set, None)
    print("Metadata template created.")

    output_file_handles = []
    try:
        for i in range(total_workers):
            worker_file_path = output_dir / f"worker_{i}_data.jsonl"
            output_file_handles.append(open(worker_file_path, "w", encoding="utf-8"))
            print(f"  Opened for writing: {worker_file_path}")

        processed_data_lines = 0
        written_to_workers = [0] * total_workers
        last_progress_len = 0
        progress_update_interval = 100000

        with open(input_tsv_file, "r", newline="", encoding="utf-8") as infile_text:
            reader = csv.DictReader(infile_text, delimiter="\t")

            for row_data_dict in reader:
                processed_data_lines += 1

                node_val = row_data_dict.get(TSV_NODE_COL)
                intf_val = row_data_dict.get(TSV_INTERFACE_COL)
                if not node_val or not intf_val:
                    continue

                series_id_str = f"{node_val}|{intf_val}"
                hash_val = zlib.adler32(series_id_str.encode("utf-8"))
                worker_idx = hash_val % total_workers

                temp_doc = {}
                metadata_subdoc = copy.deepcopy(meta_template)

                ts_val = row_data_dict.get(TSV_TIMESTAMP_COL)
                if ts_val and (parsed_datetime := parse_timestamp(ts_val)):
                    temp_doc[MONGO_TIME_FIELD] = parsed_datetime.isoformat()
                else:
                    continue

                # --- Document Construction with Type and Shape Enforcement ---
                for key, value in row_data_dict.items():
                    if key == TSV_TIMESTAMP_COL or key in DISCARD_TSV_FIELDS:
                        continue

                    is_empty = value is None or value == ""

                    if key.startswith("values."):
                        new_key = key.replace(".", "_")
                        if is_empty:
                            temp_doc[new_key] = (
                                0.0  # Use 0.0 for missing values to ensure float type
                            )
                        else:
                            try:
                                temp_doc[new_key] = float(value)
                            except (ValueError, TypeError):
                                temp_doc[new_key] = 0.0  # Use 0.0 on conversion error
                    else:  # This is a metadata field
                        target_meta_path = None
                        if key == TSV_NODE_COL:
                            target_meta_path = "device"
                        elif key == TSV_INTERFACE_COL:
                            target_meta_path = "interfaceName"
                        elif key.startswith("meta."):
                            target_meta_path = key.replace("meta.", "", 1)
                        else:
                            target_meta_path = key

                        # Use value if not empty, otherwise keep the template's None
                        if not is_empty and target_meta_path:
                            set_nested_value(metadata_subdoc, target_meta_path, value)

                if (
                    metadata_subdoc.get("device") is None
                    or metadata_subdoc.get("interfaceName") is None
                ):
                    continue

                # Create a canonically ordered metadata object by sorting its keys
                ordered_metadata = OrderedDict(sorted(metadata_subdoc.items()))
                temp_doc[MONGO_META_FIELD] = ordered_metadata

                # Create a canonically ordered final document by sorting its top-level keys
                final_ordered_doc = OrderedDict(sorted(temp_doc.items()))

                output_file_handles[worker_idx].write(
                    json.dumps(final_ordered_doc) + "\n"
                )
                written_to_workers[worker_idx] += 1

                if processed_data_lines % progress_update_interval == 0:
                    status_message = f"Processed: {processed_data_lines:,}"
                    if total_data_lines:
                        percentage = (processed_data_lines / total_data_lines) * 100
                        status_message += (
                            f"/{total_data_lines:,} lines ({percentage:.2f}%)"
                        )
                    sys.stdout.write(
                        "\r"
                        + status_message
                        + " " * (last_progress_len - len(status_message))
                    )
                    sys.stdout.flush()
                    last_progress_len = len(status_message)

        sys.stdout.write("\n")
        print("Pre-computation complete. Summary of lines written per worker file:")
        for i, count in enumerate(written_to_workers):
            print(
                f"  Worker {i} file ({output_dir / f'worker_{i}_data.jsonl'}): {count:,} lines"
            )

    except Exception as e:
        sys.stdout.write("\n")
        print(f"An error occurred during pre-computation: {e}", file=sys.stderr)
    finally:
        for fh in output_file_handles:
            if fh:
                fh.close()
        print("All output files closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pre-compute TSV data for MongoDB with a single canonical schema."
    )
    parser.add_argument(
        "--input_tsv_file",
        type=Path,
        required=True,
        help="Path to the large input TSV file.",
    )
    parser.add_argument(
        "--output_dir",
        type=Path,
        required=True,
        help="Directory to store worker-specific .jsonl files.",
    )
    parser.add_argument(
        "--total_workers",
        type=int,
        required=True,
        help="Total number of worker files to create.",
    )
    parser.add_argument(
        "--total_data_lines",
        type=int,
        default=None,
        help="Optional: Total data lines in input TSV for progress %.",
    )

    args = parser.parse_args()
    precompute_main(
        args.input_tsv_file, args.output_dir, args.total_workers, args.total_data_lines
    )
