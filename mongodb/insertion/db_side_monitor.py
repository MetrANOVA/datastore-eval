import argparse
import csv
import datetime
import signal
import sys
import time
from pathlib import Path

from pymongo import MongoClient, errors

keep_running = True
final_report_generated = False


def signal_handler(sig, frame):
    global keep_running
    if keep_running:
        # Clear the current line before printing
        sys.stdout.write(
            "\r" + " " * getattr(sys.stdout, "_last_display_len", 0) + "\r"
        )
        print("SIGINT received. Finalizing report and shutting down...")
        sys.stdout.flush()
        keep_running = False
    else:
        print("Forcing exit...")
        sys.exit(1)


def _clear_previous_line(last_len):
    """Helper to clear the previous line in stdout."""
    sys.stdout.write("\r" + " " * last_len + "\r")
    sys.stdout.flush()


def monitor_mongodb_inserts(
    uri: str,
    db_name: str,
    collection_name: str,
    interval: float,
    output_csv_path: Path = None,
    idle_timeout_polls: int = 5,  # New argument
):
    global keep_running, final_report_generated
    final_report_generated = False
    sys.stdout._last_display_len = 0  # Initialize for clearing line

    csv_writer = None
    csv_file_handle = None

    print(f"Attempting to connect to MongoDB at {uri}...")
    try:
        client = MongoClient(uri)
        client.admin.command("ping")
        db = client[db_name]
        collection = db[collection_name]
        print(
            f"Successfully connected to DB: '{db_name}', Collection: '{collection_name}'."
        )
    except errors.ConnectionFailure as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during connection: {e}", file=sys.stderr)
        sys.exit(1)

    if output_csv_path:
        try:
            print(f"Logging periodic measurements to: {output_csv_path}")
            output_csv_path.parent.mkdir(parents=True, exist_ok=True)
            csv_file_handle = open(output_csv_path, "w", newline="", encoding="utf-8")
            csv_writer = csv.writer(csv_file_handle)
            csv_header = [
                "measurement_wall_time_utc",
                "script_elapsed_seconds",
                "active_elapsed_seconds",
                "current_total_docs",
                "delta_docs_interval",
                "delta_time_interval",
                "current_rate_docs_sec",
                "total_docs_active_period",
                "overall_avg_docs_sec_active",
            ]
            csv_writer.writerow(csv_header)
            csv_file_handle.flush()
        except Exception as e:
            print(
                f"Error setting up CSV logging: {e}. CSV logging disabled.",
                file=sys.stderr,
            )
            csv_writer = None
            if csv_file_handle:
                csv_file_handle.close()

    print(f"Monitoring insert rate every {interval} second(s).")
    if idle_timeout_polls > 0:
        print(
            f"Will auto-exit after {idle_timeout_polls} idle polls once inserts have started."
        )
    else:
        print("Auto-exit on idle is disabled. Press Ctrl+C to stop.")
    print("Press Ctrl+C to stop and see summary at any time.")

    initial_doc_count_script_start = 0
    try:
        initial_doc_count_script_start = collection.count_documents({})
    except Exception as e:
        print(
            f"Error getting initial document count: {e}. Assuming 0.", file=sys.stderr
        )

    script_start_time_monotonic = time.monotonic()
    script_start_wall_time = datetime.datetime.now(datetime.timezone.utc)

    previous_doc_count = initial_doc_count_script_start
    previous_time_monotonic = script_start_time_monotonic

    data_insertion_active = False
    active_insertion_start_time_monotonic = None
    doc_count_at_active_start = 0
    consecutive_idle_polls = 0

    try:
        while keep_running:
            time.sleep(interval)
            if not keep_running:
                break  # Check flag again after sleep, if SIGINT was caught

            current_time_monotonic = time.monotonic()
            measurement_wall_time_utc = datetime.datetime.now(datetime.timezone.utc)
            current_doc_count = 0
            try:
                current_doc_count = collection.count_documents({})
            except Exception as e:
                _clear_previous_line(sys.stdout._last_display_len)
                print(
                    f"Error getting document count: {e}. Retrying...", file=sys.stderr
                )
                sys.stdout._last_display_len = 0
                continue

            delta_docs_poll = current_doc_count - previous_doc_count
            delta_time_poll = current_time_monotonic - previous_time_monotonic
            current_rate_docs_sec = 0.0
            if delta_time_poll > 0:
                current_rate_docs_sec = delta_docs_poll / delta_time_poll

            if not data_insertion_active and delta_docs_poll > 0:
                data_insertion_active = True
                active_insertion_start_time_monotonic = previous_time_monotonic
                doc_count_at_active_start = previous_doc_count
                _clear_previous_line(sys.stdout._last_display_len)
                print(
                    f"INFO: Data insertion detected at {measurement_wall_time_utc.isoformat()}. Overall average calculation for active period started."
                )
                sys.stdout._last_display_len = 0  # Reset for next display line

            overall_avg_docs_sec_active = 0.0
            total_docs_inserted_active_period = 0
            active_period_elapsed_time = 0.0

            if data_insertion_active:
                active_period_elapsed_time = (
                    current_time_monotonic - active_insertion_start_time_monotonic
                )
                total_docs_inserted_active_period = (
                    current_doc_count - doc_count_at_active_start
                )
                if active_period_elapsed_time > 0:
                    overall_avg_docs_sec_active = (
                        total_docs_inserted_active_period / active_period_elapsed_time
                    )

                # --- Auto-exit logic ---
                if delta_docs_poll == 0:
                    consecutive_idle_polls += 1
                else:
                    consecutive_idle_polls = 0  # Reset on activity

                if (
                    idle_timeout_polls > 0
                    and consecutive_idle_polls >= idle_timeout_polls
                ):
                    _clear_previous_line(sys.stdout._last_display_len)
                    print(
                        f"INFO: No new documents inserted for {consecutive_idle_polls} poll(s) (threshold: {idle_timeout_polls}). Test assumed complete."
                    )
                    sys.stdout._last_display_len = 0
                    keep_running = False  # Trigger graceful shutdown
                    # No break here, let loop condition and finally block handle exit

            total_script_elapsed_time = (
                current_time_monotonic - script_start_time_monotonic
            )
            total_docs_inserted_script_start = (
                current_doc_count - initial_doc_count_script_start
            )

            display_string = (
                f"Time Elapsed: {total_script_elapsed_time:7.2f}s | "
                f"Total Docs: {current_doc_count:,} ({'+' if total_docs_inserted_script_start >=0 else ''}{total_docs_inserted_script_start:,}) | "
                f"Rate (current): {current_rate_docs_sec:7.2f} d/s | "
                f"Avg Rate (active): {overall_avg_docs_sec_active:7.2f} d/s | "
                f"Idle Polls: {consecutive_idle_polls if data_insertion_active and idle_timeout_polls > 0 else 'N/A'}  "
            )

            _clear_previous_line(sys.stdout._last_display_len)
            sys.stdout.write(display_string)
            sys.stdout.flush()
            sys.stdout._last_display_len = len(display_string)

            if csv_writer:
                csv_row = [
                    measurement_wall_time_utc.isoformat(),
                    round(total_script_elapsed_time, 3),
                    (
                        round(active_period_elapsed_time, 3)
                        if data_insertion_active
                        else 0.0
                    ),
                    current_doc_count,
                    delta_docs_poll,
                    round(delta_time_poll, 3),
                    round(current_rate_docs_sec, 3),
                    total_docs_inserted_active_period if data_insertion_active else 0,
                    (
                        round(overall_avg_docs_sec_active, 3)
                        if data_insertion_active
                        else 0.0
                    ),
                ]
                try:
                    csv_writer.writerow(csv_row)
                    if csv_file_handle:
                        csv_file_handle.flush()
                except Exception as e:
                    _clear_previous_line(
                        sys.stdout._last_display_len
                    )  # Clear status line
                    print(f"\nError writing to CSV: {e}", file=sys.stderr)
                    sys.stdout._last_display_len = 0  # Reset

            previous_doc_count = current_doc_count
            previous_time_monotonic = current_time_monotonic

    except KeyboardInterrupt:  # Should be caught by signal handler ideally
        if keep_running:  # If signal handler didn't run first for some reason
            _clear_previous_line(sys.stdout._last_display_len)
            print("\nKeyboardInterrupt during monitoring. Finalizing...")
            keep_running = False
    except Exception as e:
        _clear_previous_line(sys.stdout._last_display_len)
        print(f"\nAn error occurred during monitoring: {e}", file=sys.stderr)
    finally:
        # Ensure the last display line is cleared before printing the final summary
        if sys.stdout._last_display_len > 0:  # Check if there was something to clear
            _clear_previous_line(sys.stdout._last_display_len)
            sys.stdout._last_display_len = 0

        if not final_report_generated:
            print(
                "\n" + "-" * 20 + " FINAL SUMMARY " + "-" * 20
            )  # Ensure newline before summary
            # ... (Final summary print logic remains the same as previous version) ...
            final_time_monotonic = time.monotonic()
            final_doc_count = initial_doc_count_script_start
            try:
                final_doc_count = collection.count_documents({})
            except Exception as e:
                print(
                    f"Error getting final doc count: {e}. Using last known.",
                    file=sys.stderr,
                )
                final_doc_count = previous_doc_count
            total_script_duration_final = (
                final_time_monotonic - script_start_time_monotonic
            )
            total_docs_inserted_script_final = (
                final_doc_count - initial_doc_count_script_start
            )
            print(f"Monitoring Started (UTC): {script_start_wall_time.isoformat()}")
            print(
                f"Monitoring Ended   (UTC): {datetime.datetime.now(datetime.timezone.utc).isoformat()}"
            )
            print(
                f"Total Monitoring Duration: {total_script_duration_final:.2f} seconds"
            )
            print(f"Initial Document Count: {initial_doc_count_script_start:,}")
            print(f"Final Document Count:   {final_doc_count:,}")
            print(
                f"Total Documents Inserted During Monitoring Period: {total_docs_inserted_script_final:,}"
            )
            if (
                data_insertion_active
            ):  # Ensure active_insertion_start_time_monotonic is not None
                active_period_duration_final = final_time_monotonic - (
                    active_insertion_start_time_monotonic
                    if active_insertion_start_time_monotonic
                    else script_start_time_monotonic
                )
                docs_inserted_active_final = final_doc_count - (
                    doc_count_at_active_start
                    if active_insertion_start_time_monotonic
                    else initial_doc_count_script_start
                )
                overall_avg_docs_sec_final_active = 0.0
                if active_period_duration_final > 0 and docs_inserted_active_final > 0:
                    overall_avg_docs_sec_final_active = (
                        docs_inserted_active_final / active_period_duration_final
                    )
                print(
                    f"Duration of Active Insertion: {active_period_duration_final:.2f} seconds"
                )
                print(
                    f"Documents Inserted During Active Period: {docs_inserted_active_final:,}"
                )
                print(
                    f"Overall Average Insertion Rate (During Active Period): {overall_avg_docs_sec_final_active:.2f} docs/s"
                )
            else:
                print("No data insertion was detected during the monitoring period.")
            print("-" * 55)
            final_report_generated = True

        if csv_file_handle:
            try:
                csv_file_handle.close()
                print(f"Measurement CSV '{output_csv_path}' closed.")
            except Exception as e:
                print(f"Error closing CSV file: {e}", file=sys.stderr)
        if (
            "client" in locals() and client
        ):  # Ensure client exists before trying to close
            try:
                client.close()
                print("MongoDB connection closed.")
            except Exception as e:
                print(f"Error closing MongoDB connection: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Monitor MongoDB insertion rate, log to CSV, and auto-exit on idle.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--uri", default="mongodb://localhost:27017/", help="MongoDB connection URI."
    )
    parser.add_argument("--db", required=True, help="MongoDB Database name.")
    parser.add_argument(
        "--collection", required=True, help="MongoDB Collection name to monitor."
    )
    parser.add_argument(
        "--interval", type=float, default=1.0, help="Polling interval in seconds."
    )
    parser.add_argument(
        "--output_csv",
        type=Path,
        default=None,
        help="Optional: Path to CSV file to log periodic measurements.",
    )
    parser.add_argument(
        "--idle_timeout_polls",
        type=int,
        default=5,
        help="Number of consecutive idle polls (after inserts start) before auto-exiting. "
        "Set to 0 to disable auto-exit and rely on Ctrl+C.",
    )

    args = parser.parse_args()
    if args.interval <= 0:
        print("Error: --interval must be positive.", file=sys.stderr)
        sys.exit(1)
    if args.idle_timeout_polls < 0:
        print("Error: --idle_timeout_polls cannot be negative.", file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    monitor_mongodb_inserts(
        args.uri,
        args.db,
        args.collection,
        args.interval,
        args.output_csv,
        args.idle_timeout_polls,
    )
    print("Monitoring script finished.")
