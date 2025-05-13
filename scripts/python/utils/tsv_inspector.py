# inspect_tsv_line_by_line.py

import argparse
import csv
import sys
from pathlib import Path


def inspect_tsv(tsv_file_path: Path):
    """
    Interactively reads a TSV file line by line, displaying header:value pairs.

    Args:
        tsv_file_path: Path to the input TSV file.
    """
    try:
        print(f"Opening TSV file: {tsv_file_path}")
        with open(tsv_file_path, "r", newline="", encoding="utf-8") as tsvfile:
            # Create a CSV reader specific for Tab Separated Values
            reader = csv.reader(tsvfile, delimiter="\t")

            # --- Read Header ---
            try:
                header = [h.strip() for h in next(reader)]
                if not header:
                    print("Error: Header row is empty.", file=sys.stderr)
                    return  # Exit function
                print(f"\n--- Header ---")
                print(f"Found {len(header)} columns:")
                for i, col_name in enumerate(header):
                    print(f"  {i+1}. {col_name}")
                print("--------------\n")
            except StopIteration:
                print(
                    f"Error: File '{tsv_file_path}' appears to be empty.",
                    file=sys.stderr,
                )
                return
            except Exception as e:
                print(
                    f"Error reading header from '{tsv_file_path}': {e}", file=sys.stderr
                )
                return

            # --- Interactive Loop for Data Rows ---
            line_number = 1  # Start counting data lines after header
            print("Starting data inspection...")

            # Iterate through the reader - this reads line by line efficiently
            for row in reader:
                line_number += 1
                print(f"--- Line {line_number} ---")

                # Check for column count mismatch (common issue)
                if len(row) != len(header):
                    print(
                        f"Warning: Line {line_number} has {len(row)} columns, but header has {len(header)}.",
                        file=sys.stderr,
                    )
                    print("Displaying data based on available columns/headers:")

                # Use zip to pair header names with row values
                # zip stops automatically at the shortest length, handling mismatches reasonably
                for col_name, value in zip(header, row):
                    print(f"  {col_name}: {value}")

                # Handle case where row is longer than header (optional)
                # if len(row) > len(header):
                #    print("  (Extra values in row):", row[len(header):])

                print("-" * (10 + len(str(line_number))))  # Dynamic separator length

                # --- Wait for User Input ---
                try:
                    user_input = (
                        input("Press Enter for next line, or 'q' to quit: ")
                        .strip()
                        .lower()
                    )
                    if user_input == "q":
                        print("\nQuitting as requested.")
                        return  # Exit function
                except KeyboardInterrupt:
                    print("\nCtrl+C detected. Quitting.")
                    return  # Exit on Ctrl+C

            # --- End of File ---
            print("\n--- End of file reached. ---")

    except FileNotFoundError:
        print(f"Error: Input TSV file not found at '{tsv_file_path}'", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Interactively inspect large TSV files line by line.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--tsv-file",
        required=True,
        type=Path,
        help="Path to the input TSV file to inspect.",
    )

    args = parser.parse_args()
    inspect_tsv(args.tsv_file)
    print("Script finished.")
