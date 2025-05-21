# split_for_worker_sets_single_pass.py (Corrected to pass max_cache_size)
import argparse
import csv
import os
import shutil  # For rmtree
import sys
import zlib  # For adler32 hash function
from collections import OrderedDict  # For LRU cache
from pathlib import Path

# --- Configuration ---
# Default values, can be overridden by args
OUTPUT_BUFFER_SIZE_LINES = 10000
MAX_OPEN_FILES_CACHE_SIZE = 500


def flush_buffer(
    filepath: Path,
    lines_buffer: list,
    header_list: list,
    headers_written_set: set,
    open_file_cache: OrderedDict,  # LRU cache of (file_handle, csv_writer)
    max_cache_size: int,  # This parameter is defined
):
    """
    Writes a buffer of lines to the specified file, managing open file handles
    via an LRU cache. Adds header if new.
    """
    if not lines_buffer:
        return

    csv_writer = None  # Initialize to None

    if filepath in open_file_cache:
        _file_handle, csv_writer = open_file_cache[filepath]  # Unpack tuple
        open_file_cache.move_to_end(filepath)  # Mark as recently used
    else:
        if len(open_file_cache) >= max_cache_size:
            lru_filepath, (lru_handle_to_close, _lru_writer_to_discard) = (
                open_file_cache.popitem(last=False)
            )
            try:
                lru_handle_to_close.close()
            except Exception as e:
                print(
                    f"\nWarning: Error closing LRU file {lru_filepath}: {e}",
                    file=sys.stderr,
                )

        mode = "w" if filepath not in headers_written_set else "a"
        file_handle_new = None
        try:
            file_handle_new = open(filepath, mode, newline="", encoding="utf-8")
            csv_writer = csv.writer(file_handle_new, delimiter="\t")
            if mode == "w":
                csv_writer.writerow(header_list)
                headers_written_set.add(filepath)
            open_file_cache[filepath] = (file_handle_new, csv_writer)
        except Exception as e:
            print(f"\nError opening/preparing file {filepath}: {e}", file=sys.stderr)
            if file_handle_new:
                try:
                    file_handle_new.close()
                except:
                    pass
            return

    try:
        if csv_writer:
            csv_writer.writerows(lines_buffer)
        else:
            print(
                f"\nError: CSV writer not available for {filepath} during write attempt.",
                file=sys.stderr,
            )
    except Exception as e:
        print(f"\nError writing buffer to {filepath}: {e}", file=sys.stderr)


def split_main_single_pass(
    input_file: Path,
    output_base_dir: Path,
    max_num_workers: int,
    node_col_name: str,
    intf_col_name: str,
    total_data_lines: int,
    clear_output_dir: bool,
    run_only_max_workers_scenario: bool,
    buffer_size_lines_arg: int,
    max_open_files_arg: int,
):
    # Use local variables for these settings based on args
    current_output_buffer_size_lines = buffer_size_lines_arg
    current_max_open_files_cache_size = max_open_files_arg

    print(f"Using output buffer size: {current_output_buffer_size_lines} lines")
    print(f"Max open output files cache size: {current_max_open_files_cache_size}")

    if not input_file.is_file():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)
    if total_data_lines is not None and total_data_lines <= 0:
        print(
            "Error: --total_data_lines, if provided, must be positive.", file=sys.stderr
        )
        sys.exit(1)
    if clear_output_dir and output_base_dir.exists():
        print(f"Clearing existing output directory: {output_base_dir}")
        try:
            shutil.rmtree(output_base_dir)
        except Exception as e:
            print(f"Error removing directory {output_base_dir}: {e}", file=sys.stderr)
            sys.exit(1)

    print(f"Input file: {input_file}")
    print(f"Output base directory: {output_base_dir}")
    if run_only_max_workers_scenario:
        print(
            f"Single scenario mode: Generating files only for {max_num_workers} workers."
        )
    else:
        print(
            f"Multi-scenario mode: Generating files for 2 to {max_num_workers} workers."
        )
    if total_data_lines is not None:
        print(f"Total data lines to process (as provided): {total_data_lines}")
    else:
        print(
            "Total data lines not provided; progress will be shown in lines processed."
        )

    header_list = None
    try:
        with open(input_file, "r", newline="", encoding="utf-8") as infile_peek_header:
            reader = csv.reader(infile_peek_header, delimiter="\t")
            header_list = next(reader)
        if not header_list:
            print("Error: Could not read header from input file.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error reading header: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        node_col_idx = header_list.index(node_col_name)
        intf_col_idx = header_list.index(intf_col_name)
    except ValueError:
        print(
            f"Error: Node ('{node_col_name}') or Interface ('{intf_col_name}') column not in header: {header_list}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Header identified. Starting single pass splitting...")

    if run_only_max_workers_scenario:
        scenarios_to_process = range(max_num_workers, max_num_workers + 1)
    else:
        scenarios_to_process = range(2, max_num_workers + 1)

    output_buffers = {}
    headers_written_to_output_files = set()
    series_assignment_cache = {}
    open_file_handle_cache = OrderedDict()

    for num_scenario_workers_val in scenarios_to_process:
        scenario_dir = output_base_dir / f"{num_scenario_workers_val}_workers"
        scenario_dir.mkdir(parents=True, exist_ok=True)
        for i in range(num_scenario_workers_val):
            output_filepath_key = (
                scenario_dir / f"{num_scenario_workers_val}_workers_worker_{i + 1}.tsv"
            )
            if output_filepath_key not in output_buffers:
                output_buffers[output_filepath_key] = []

    processed_data_lines = 0
    last_progress_len = 0
    progress_update_interval = 10000
    if total_data_lines and total_data_lines > 1000000:
        progress_update_interval = max(10000, total_data_lines // 100)

    try:
        with open(input_file, "r", newline="", encoding="utf-8") as infile_text:
            reader = csv.reader(infile_text, delimiter="\t")
            next(reader)  # Skip header row

            for row_list in reader:
                processed_data_lines += 1
                try:
                    node_val = row_list[node_col_idx]
                    intf_val = row_list[intf_col_idx]
                except IndexError:
                    sys.stderr.write(
                        f"\nWarning: Data line {processed_data_lines} malformed. Skipping.\n"
                    )
                    continue

                series_id_str = f"{node_val}|{intf_val}"
                if series_id_str not in series_assignment_cache:
                    hash_val = zlib.adler32(series_id_str.encode("utf-8"))
                    assignments = {}
                    for num_potential_scenarios in range(2, max_num_workers + 1):
                        assignments[num_potential_scenarios] = (
                            hash_val % num_potential_scenarios
                        )
                    series_assignment_cache[series_id_str] = assignments

                worker_assignments_for_series = series_assignment_cache[series_id_str]

                for num_scenario_workers_iter in scenarios_to_process:
                    target_worker_index = worker_assignments_for_series.get(
                        num_scenario_workers_iter
                    )
                    if target_worker_index is None:
                        continue

                    scenario_dir = (
                        output_base_dir / f"{num_scenario_workers_iter}_workers"
                    )
                    output_filepath = (
                        scenario_dir
                        / f"{num_scenario_workers_iter}_workers_worker_{target_worker_index + 1}.tsv"
                    )

                    output_buffers[output_filepath].append(list(row_list))

                    if (
                        len(output_buffers[output_filepath])
                        >= current_output_buffer_size_lines
                    ):
                        flush_buffer(
                            output_filepath,
                            output_buffers[output_filepath],
                            header_list,
                            headers_written_to_output_files,
                            open_file_handle_cache,
                            current_max_open_files_cache_size,
                        )  # Corrected: Pass max_cache_size
                        output_buffers[output_filepath] = []

                if processed_data_lines % progress_update_interval == 0 or (
                    total_data_lines is not None
                    and processed_data_lines == total_data_lines
                ):
                    if total_data_lines is not None and total_data_lines > 0:
                        percentage = (processed_data_lines / total_data_lines) * 100
                        status_message = (
                            f"Processed: {processed_data_lines:,}/{total_data_lines:,} lines ({percentage:.2f}%) "
                            f"- Series Cached: {len(series_assignment_cache)} "
                            f"- Open Files: {len(open_file_handle_cache)}"
                        )
                    else:
                        status_message = (
                            f"Processed: {processed_data_lines:,} lines "
                            f"- Series Cached: {len(series_assignment_cache)} "
                            f"- Open Files: {len(open_file_handle_cache)}"
                        )
                    current_line_len = len(status_message)
                    sys.stdout.write(
                        "\r"
                        + status_message
                        + " " * (last_progress_len - current_line_len)
                    )
                    sys.stdout.flush()
                    last_progress_len = current_line_len

        sys.stdout.write("\n")
        print("Flushing all remaining buffers and closing cached files...")
        for output_filepath_key, line_buffer_val in output_buffers.items():
            if line_buffer_val:
                flush_buffer(
                    output_filepath_key,
                    line_buffer_val,
                    header_list,
                    headers_written_to_output_files,
                    open_file_handle_cache,
                    current_max_open_files_cache_size,
                )  # Corrected: Pass max_cache_size

        for filepath_key_final in list(open_file_handle_cache.keys()):
            file_handle_final, _csv_writer_final = open_file_handle_cache.pop(
                filepath_key_final
            )
            try:
                file_handle_final.close()
            except Exception as e:
                print(
                    f"\nWarning: Error closing final cached file {filepath_key_final}: {e}",
                    file=sys.stderr,
                )

    except Exception as e:
        sys.stdout.write("\n")
        print(f"An error occurred during processing: {e}", file=sys.stderr)
    finally:
        sys.stdout.write("\n")
        # Ensure all cached handles are attempted to be closed, even if an error occurred mid-loop
        for filepath_key_exc in list(open_file_handle_cache.keys()):
            file_handle_exc, _ = open_file_handle_cache.pop(filepath_key_exc)
            try:
                file_handle_exc.close()
            except:
                pass

        print(f"Splitting complete. Processed {processed_data_lines} data lines.")
        for num_scenario_workers_final in scenarios_to_process:
            s_dir = output_base_dir / f"{num_scenario_workers_final}_workers"
            if s_dir.exists():
                num_files = len(list(s_dir.glob("*.tsv")))
                print(
                    f"  Scenario {num_scenario_workers_final} workers: {num_files} files created in {s_dir}"
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Split a large TSV into multiple sets of files for N-worker scenarios "
        "in a single pass, using an LRU cache for open file handles.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input_file",
        type=Path,
        required=True,
        help="Path to the large input TSV file.",
    )
    parser.add_argument(
        "--output_base_dir",
        type=Path,
        required=True,
        help="Base directory to store the split TSV file sets.",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        required=True,
        help="Maximum number of workers (N) to create file sets for.",
    )
    parser.add_argument(
        "--total_data_lines",
        type=int,
        default=None,
        help="Optional: Total number of DATA lines in input file.",
    )
    parser.add_argument(
        "--node_col_name",
        type=str,
        default="meta.device",
        help="TSV header column for node/device.",
    )
    parser.add_argument(
        "--intf_col_name",
        type=str,
        default="meta.name",
        help="TSV header column for interface/name.",
    )
    parser.add_argument(
        "--clear_output_dir",
        action="store_true",
        help="If set, remove output_base_dir if it exists.",
    )
    parser.add_argument(
        "--run_only_max_workers_scenario",
        action="store_true",
        help="If set, only generates files for the scenario with --max_workers.",
    )
    parser.add_argument(
        "--buffer_size_lines",
        type=int,
        default=OUTPUT_BUFFER_SIZE_LINES,
        help=f"Lines to buffer per output file before flushing. Default: {OUTPUT_BUFFER_SIZE_LINES}",
    )
    parser.add_argument(
        "--max_open_files",
        type=int,
        default=MAX_OPEN_FILES_CACHE_SIZE,
        help=f"Max output files to keep open. Default: {MAX_OPEN_FILES_CACHE_SIZE}",
    )

    args = parser.parse_args()

    min_workers_for_script = 1 if args.run_only_max_workers_scenario else 2
    if args.max_workers < min_workers_for_script:
        print(
            f"Error: --max_workers must be at least {min_workers_for_script}.",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.total_data_lines is not None and args.total_data_lines < 0:
        print("Error: --total_data_lines cannot be negative.", file=sys.stderr)
        sys.exit(1)
    if (
        args.input_file.resolve() == args.output_base_dir.resolve()
        or args.output_base_dir.resolve() in args.input_file.resolve().parents
    ):
        print(
            "Error: Output base directory cannot be input file or parent of input.",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.buffer_size_lines < 1:
        print("Error: --buffer_size_lines must be positive.", file=sys.stderr)
        sys.exit(1)
    if args.max_open_files < 1:
        print("Error: --max_open_files must be positive.", file=sys.stderr)
        sys.exit(1)

    split_main_single_pass(
        args.input_file,
        args.output_base_dir,
        args.max_workers,
        args.node_col_name,
        args.intf_col_name,
        args.total_data_lines,
        args.clear_output_dir,
        args.run_only_max_workers_scenario,
        args.buffer_size_lines,
        args.max_open_files,
    )
