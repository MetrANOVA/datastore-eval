import argparse
import csv
import datetime
import json
import random
import subprocess
import sys
import time


def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Database Performance Test Orchestrator"
    )
    parser.add_argument(
        "--query-file",
        required=True,
        help="Path to the .txt file containing the query pipeline.",
    )
    parser.add_argument(
        "--db-type",
        required=True,
        choices=["mongo", "clickhouse"],
        help="Type of database to test.",
    )
    parser.add_argument(
        "--ip", required=True, help="IP address of the database server."
    )
    parser.add_argument("--db-name", required=True, help="Name of the database.")
    parser.add_argument(
        "--table", required=True, help="Name of the table or collection."
    )
    parser.add_argument(
        "--warmup-seconds",
        required=True,
        type=int,
        help="Duration of the warm-up phase in seconds.",
    )
    parser.add_argument(
        "--test-duration-seconds",
        required=True,
        type=int,
        help="Duration of the main testing phase in seconds.",
    )
    parser.add_argument(
        "--output-file", required=True, help="Path to save the output CSV file."
    )
    return parser.parse_args()


def run_single_query(db_type, ip, db_name, table, query_filename):
    """
    Calls the runner script with the query filename.
    """
    runner_script = f"runners/{db_type}_runner.py"
    try:
        result = subprocess.run(
            ["python", runner_script, ip, db_name, table, query_filename],
            capture_output=True,
            text=True,
            check=True,
        )

        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)

        data = json.loads(result.stdout)
        return data["exec_time"], data["result"]
    except subprocess.CalledProcessError as e:
        if e.stderr:
            print(e.stderr, end="", file=sys.stderr)
        print(
            f"ERROR running {query_filename}: Process exited with error.",
            flush=True,
            file=sys.stderr,
        )
        return None, None


def main():
    args = parse_args()

    print("--- Test Configuration ---")
    print(f"Database Type: {args.db_type}")
    print(f"Query File: {args.query_file}")
    print(f"Target: {args.ip}/{args.db_name}/{args.table}")
    print(f"Output File: {args.output_file}")
    print("--------------------------\n")

    # --- NEW: Upfront Sanity Check ---
    print("Performing initial sanity check...")
    exec_time, query_result = run_single_query(
        args.db_type, args.ip, args.db_name, args.table, args.query_file
    )

    if query_result is None or not query_result:
        print("\nERROR: Sanity check failed. The query returned no results.")
        print("Aborting benchmark.")
        sys.exit(1)
    else:
        print("Sanity check PASSED. Query returned results.")
        print("\n" + "=" * 20 + f" SANITY CHECK DATA " + "=" * 20)
        print(json.dumps(query_result[:5], indent=2))
        if len(query_result) > 5:
            print(f"... and {len(query_result) - 5} more records.")
        print("=" * 59 + "\n")
    # --- End of Sanity Check ---

    print(f"Starting warm-up for {args.warmup_seconds} seconds...")
    warmup_end_time = time.time() + args.warmup_seconds
    while time.time() < warmup_end_time:
        run_single_query(
            args.db_type, args.ip, args.db_name, args.table, args.query_file
        )
    print("Warm-up complete.\n")

    print(f"Starting test for {args.test_duration_seconds} seconds...")
    all_results = []
    test_end_time = time.time() + args.test_duration_seconds
    query_count = 0
    while time.time() < test_end_time:
        exec_time, query_result = run_single_query(
            args.db_type, args.ip, args.db_name, args.table, args.query_file
        )

        if exec_time is not None:
            query_count += 1
            all_results.append(
                {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "query_name": args.query_file,
                    "exec_time_s": exec_time,
                }
            )

    print(f"Test complete. Executed {query_count} queries.\n")

    if not all_results:
        print("No results to save.")
        return

    with open(args.output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
        writer.writeheader()
        writer.writerows(all_results)

    print(f"Benchmark results saved to {args.output_file}")


if __name__ == "__main__":
    main()
