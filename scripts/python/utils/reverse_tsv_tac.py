# reverse_tsv_order_tac.py
import argparse
import shutil  # For checking if tac exists
import subprocess
import sys
from pathlib import Path


def reverse_tsv_with_tac(input_filepath: Path, output_filepath: Path):
    """
    Reverses a TSV file using 'head', 'tail', and 'tac' commands.
    Header is preserved.
    """
    print(f"Reversing TSV file using 'tac': {input_filepath} -> {output_filepath}")

    if not input_filepath.is_file():
        print(f"Error: Input file not found: {input_filepath}", file=sys.stderr)
        return False

    # Check if tac, head, tail commands are available
    for cmd in ["tac", "head", "tail"]:
        if not shutil.which(cmd):
            print(
                f"Error: Required command '{cmd}' not found in your PATH.",
                file=sys.stderr,
            )
            print(
                "This script relies on standard Unix utilities. For other systems, consider the pure Python version.",
                file=sys.stderr,
            )
            return False

    try:
        # 1. Extract header
        head_process = subprocess.run(
            ["head", "-n", "1", str(input_filepath)], capture_output=True, check=True
        )
        header_content = head_process.stdout
        if not header_content.strip():  # Check if header is empty or just newline
            print(
                f"Warning: Header in {input_filepath} appears empty or file has only one line.",
                file=sys.stderr,
            )
            # If file is just one line, tac will reverse it to itself.
            # If it's truly empty, head will output nothing.
            if not header_content:  # File was completely empty
                with open(output_filepath, "wb") as outfile:
                    outfile.write(b"")  # Create an empty output file
                print("Input file was empty. Output file is empty.")
                return True

        # 2. Write header to output file
        with open(output_filepath, "wb") as outfile:
            outfile.write(header_content)  # head captures the newline too

        # 3. Reverse data lines (all lines except header) and append
        # Create a pipeline: tail -n +2 input_file | tac >> output_file_append_mode
        # We use Popen to manage the pipeline explicitly

        print("Processing data lines (this might take a while for large files)...")
        # Start tail process (skip header)
        tail_process = subprocess.Popen(
            ["tail", "-n", "+2", str(input_filepath)], stdout=subprocess.PIPE
        )

        # Open output file in append binary mode
        with open(output_filepath, "ab") as outfile_append:
            # Start tac process, taking input from tail's output, writing to outfile_append
            tac_process = subprocess.Popen(
                ["tac"],
                stdin=tail_process.stdout,
                stdout=outfile_append,  # Write directly to the file opened in append mode
            )
            # Allow tail_process to receive a SIGPIPE if tac_process exits.
            if tail_process.stdout:
                tail_process.stdout.close()

            tac_return_code = tac_process.wait()

        tail_return_code = (
            tail_process.wait()
        )  # Ensure tail finishes (should be quick after stdout closed)

        if tail_return_code != 0:
            print(
                f"Warning: 'tail' process finished with code {tail_return_code}.",
                file=sys.stderr,
            )
            # This might happen if input file had only 1 line, tail -n +2 yields nothing.
            # If header_content was not empty, this is probably fine.
            if (
                not header_content.strip() and tail_return_code != 0
            ):  # truly empty file might error tail
                print(f"Tail error might be due to very small file. Check output.")
        if tac_return_code != 0:
            print(
                f"Error: 'tac' process failed with code {tac_return_code}.",
                file=sys.stderr,
            )
            # output_filepath might be partially written (only header)
            return False

        print(f"File reversal complete. Output written to {output_filepath}")
        return True

    except subprocess.CalledProcessError as e:
        print(f"Error during subprocess execution: {e}", file=sys.stderr)
        print(
            f"Stdout: {e.stdout.decode(errors='ignore') if e.stdout else 'N/A'}",
            file=sys.stderr,
        )
        print(
            f"Stderr: {e.stderr.decode(errors='ignore') if e.stderr else 'N/A'}",
            file=sys.stderr,
        )
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reverse the order of lines in a TSV file using 'tac', keeping the header first.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input_file",
        type=Path,
        required=True,
        help="Path to the input TSV file (timestamps in descending order).",
    )
    parser.add_argument(
        "--output_file",
        type=Path,
        required=True,
        help="Path to write the new TSV file (timestamps in ascending order).",
    )

    args = parser.parse_args()

    if args.input_file == args.output_file:
        print("Error: Input and output file paths must be different.", file=sys.stderr)
        sys.exit(1)

    # For this script, we directly call the tac version
    # You could add a flag to choose between python/tac versions if desired.
    success = reverse_tsv_with_tac(args.input_file, args.output_file)
    if not success:
        sys.exit(1)
