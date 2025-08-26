# validate_precomputed_data.py (Updated to count and summarize all errors)
import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


def get_schema_with_types(obj):
    """
    Recursively traverses a Python object (from JSON) and returns a parallel
    structure where values are replaced by their Python types. Uses sorted lists
    of key-value pairs for dictionaries to ensure order is part of the schema.
    """
    if isinstance(obj, dict):
        return sorted([(k, get_schema_with_types(v)) for k, v in obj.items()])
    elif isinstance(obj, list):
        return list
    else:
        return type(obj)


def compare_schemas_recursive(ref_schema, current_schema, path=""):
    """
    Recursively compares two schema structures and returns a tuple of
    (error_category, details_string) if a difference is found, else (None, None).
    """
    if len(ref_schema) != len(current_schema):
        ref_keys = {item[0] for item in ref_schema}
        current_keys = {item[0] for item in current_schema}
        diff = ref_keys.symmetric_difference(current_keys)
        return (
            "shape_mismatch",
            f"Key mismatch at path '{path}': Difference is {diff}",
        )

    for (ref_key, ref_val), (cur_key, cur_val) in zip(ref_schema, current_schema):
        new_path = f"{path}.{ref_key}" if path else ref_key
        if ref_key != cur_key:
            return (
                "order_mismatch",
                f"Key order mismatch at path '{path}': Expected key '{ref_key}', got '{cur_key}'",
            )

        if isinstance(ref_val, list):
            if not isinstance(cur_val, list):
                return (
                    "type_to_other",
                    f"Type mismatch at path '{new_path}': Expected a dictionary, got {cur_val}",
                )
            # Recurse
            diff_result = compare_schemas_recursive(ref_val, cur_val, new_path)
            if diff_result[0] is not None:  # If a difference was found in recursion
                return diff_result
        elif ref_val != cur_val:
            # Type mismatch found. Categorize it.
            error_category = (
                "type_to_none" if cur_val == type(None) else "type_to_other"
            )
            details = f"Path '{new_path}': Expected type {ref_val.__name__}, got {cur_val.__name__}"
            return (error_category, details)

    return (None, None)  # No difference found


def validate_data(
    jsonl_filepath: Path,
    meta_key: str = "meta",
    series_id_keys: list = ["device", "interfaceName"],
):
    print(
        f"Validating per-series schema (keys, order, and types) in file: {jsonl_filepath}"
    )
    print(f"Identifying series by keys within '{meta_key}': {series_id_keys}")

    if not jsonl_filepath.is_file():
        print(f"Error: File not found at '{jsonl_filepath}'", file=sys.stderr)
        sys.exit(1)

    series_reference_schemas = {}

    # Data structures for counting and summarizing errors
    error_counts = defaultdict(int)
    error_examples = {}
    inconsistent_series = set()

    processed_docs = 0
    last_progress_len = 0

    try:
        with open(jsonl_filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                processed_docs += 1

                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    print(
                        f"\nWarning: Invalid JSON on line {line_num}. Skipping.",
                        file=sys.stderr,
                    )
                    continue

                meta_object = data.get(meta_key)
                if not isinstance(meta_object, dict):
                    continue

                try:
                    series_key = tuple(meta_object[k] for k in series_id_keys)
                except KeyError:
                    print(
                        f"\nWarning: Line {line_num}: '{meta_key}' object is missing a series identifier key. Skipping.",
                        file=sys.stderr,
                    )
                    continue

                current_schema = get_schema_with_types(data)

                if series_key not in series_reference_schemas:
                    series_reference_schemas[series_key] = (current_schema, line_num)
                else:
                    reference_schema, _ref_line_num = series_reference_schemas[
                        series_key
                    ]

                    error_category, details = compare_schemas_recursive(
                        reference_schema, current_schema
                    )

                    if error_category:
                        error_counts[error_category] += 1
                        inconsistent_series.add(series_key)
                        # Store the first example of each error type
                        if error_category not in error_examples:
                            error_examples[error_category] = (details, line_num)

                # Progress update
                if processed_docs % 50000 == 0:
                    status_message = f"Checked {processed_docs:,} documents... Found {sum(error_counts.values()):,} total inconsistencies so far."
                    sys.stdout.write("\r" + " " * last_progress_len + "\r")
                    sys.stdout.write(status_message)
                    sys.stdout.flush()
                    last_progress_len = len(status_message)

        sys.stdout.write(
            "\r" + " " * last_progress_len + "\r"
        )  # Clear final progress line

        total_errors = sum(error_counts.values())
        if total_errors == 0:
            print("\n--- SUCCESS ---")
            print(
                f"Scan complete. All {processed_docs:,} documents have a consistent schema WITHIN their respective series."
            )
            print(f"Found {len(series_reference_schemas)} unique series.")
        else:
            print("\n--- FAILURE: Schema Inconsistencies Found ---")
            print(f"Scan complete. Checked {processed_docs:,} documents.")
            print(
                f"Found a total of {total_errors:,} inconsistencies across {len(inconsistent_series)} unique series."
            )
            print("\nError Summary:")
            for category, count in error_counts.items():
                print(f"  - {category}: {count:,} occurrences")
                if category in error_examples:
                    details, line_num = error_examples[category]
                    print(f"    (First example on line {line_num}: {details})")
            sys.exit(1)

    except Exception as e:
        sys.stdout.write("\n")
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check for consistent schema (keys, order, and types) for each series in a JSONL file, counting all errors.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "jsonl_file", type=Path, help="Path to the input JSONL file to validate."
    )
    parser.add_argument(
        "--meta_key",
        type=str,
        default="meta",
        help="The key of the object containing the series identifiers.",
    )
    parser.add_argument(
        "--series_id_keys",
        type=str,
        default="device,interfaceName",
        help="Comma-separated list of keys within the --meta_key object that uniquely identify a time series.",
    )

    args = parser.parse_args()
    series_id_keys_list = [key.strip() for key in args.series_id_keys.split(",")]
    validate_data(args.jsonl_file, args.meta_key, series_id_keys_list)
