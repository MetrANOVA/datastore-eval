# calculate_offsets.py
import argparse
import math  # Used only for ceil in one calculation, // and % are primary
import sys


def calculate_segments(total_data_lines: int, num_workers: int):
    """
    Calculates offset and limit values to divide total_data_lines among num_workers.

    Args:
        total_data_lines: The total number of data lines (excluding header).
        num_workers: The desired number of parallel workers.

    Returns:
        A list of dictionaries, where each dict contains 'worker_id',
        'offset', 'limit', 'start_line', 'end_line'.
        Returns None if inputs are invalid.
    """
    if total_data_lines < 0:
        print("Error: Total data lines cannot be negative.", file=sys.stderr)
        return None
    if num_workers <= 0:
        print("Error: Number of workers must be positive.", file=sys.stderr)
        return None

    if total_data_lines == 0:
        print(
            "Warning: Total data lines is 0. All workers will have limit 0.",
            file=sys.stderr,
        )

    if num_workers > total_data_lines and total_data_lines > 0:
        print(
            f"Warning: Number of workers ({num_workers}) exceeds total data lines ({total_data_lines}). "
            f"Some workers will have a limit of 0.",
            file=sys.stderr,
        )

    # Calculate base number of lines per worker and the remainder
    base_limit = total_data_lines // num_workers
    remainder = total_data_lines % num_workers

    segments = []
    current_offset = 0
    for i in range(num_workers):
        # Distribute the remainder among the first 'remainder' workers
        limit_for_this_worker = base_limit + (1 if i < remainder else 0)

        # Calculate human-readable line numbers (1-based index)
        start_line = current_offset + 1
        end_line = current_offset + limit_for_this_worker

        segments.append(
            {
                "worker_id": i + 1,
                "offset": current_offset,
                "limit": limit_for_this_worker,
                "start_line": (
                    start_line if limit_for_this_worker > 0 else current_offset
                ),  # Adjust if limit is 0
                "end_line": end_line,
            }
        )

        # Update offset for the next worker
        current_offset += limit_for_this_worker

    return segments


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate --offset and --limit values for parallel processing of a file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--total_lines",
        type=int,
        required=True,
        help="Total number of DATA lines in the file (excluding the header line).",
    )
    parser.add_argument(
        "--num_workers",
        type=int,
        required=True,
        help="The desired number of parallel workers/processes.",
    )

    args = parser.parse_args()

    # --- Validation ---
    if args.total_lines < 0:
        print("Error: --total_lines must be 0 or greater.", file=sys.stderr)
        sys.exit(1)
    if args.num_workers < 1:
        print("Error: --num_workers must be 1 or greater.", file=sys.stderr)
        sys.exit(1)

    # --- Calculation ---
    calculated_segments = calculate_segments(args.total_lines, args.num_workers)

    # --- Output ---
    if calculated_segments:
        print(
            f"\nCalculated segments for {args.total_lines} data lines across {args.num_workers} workers:"
        )
        print("-" * 70)
        print(f"{'Worker':<8} {'--offset':<12} {'--limit':<12} {'Line Range (Approx)'}")
        print("-" * 70)
        for segment in calculated_segments:
            line_range = (
                f"{segment['start_line']:,} - {segment['end_line']:,}"
                if segment["limit"] > 0
                else "N/A (limit 0)"
            )
            print(
                f"{segment['worker_id']:<8} {segment['offset']:<12,} {segment['limit']:<12,} # {line_range}"
            )
        print("-" * 70)
        print(
            "\nUse these --offset and --limit values when launching your parallel benchmark scripts."
        )
        # Verify total lines processed matches input
        total_lines_calculated = sum(s["limit"] for s in calculated_segments)
        if total_lines_calculated != args.total_lines:
            print(
                f"\nWARNING: Total lines calculated ({total_lines_calculated}) does not match input ({args.total_lines}). Check logic.",
                file=sys.stderr,
            )
