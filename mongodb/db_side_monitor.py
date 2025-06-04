import argparse
import datetime
import signal
import sys
import time

from pymongo import MongoClient, errors

# --- Global flag for graceful exit ---
keep_running = True
final_report_generated = False


def signal_handler(sig, frame):
    global keep_running
    if keep_running:
        print("\nSIGINT received. Finalizing report and shutting down...")
        keep_running = False
    else:
        print("Forcing exit...")
        sys.exit(1)


def monitor_mongodb_inserts(
    uri: str, db_name: str, collection_name: str, interval: float
):
    global keep_running, final_report_generated
    final_report_generated = False

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

    print(
        f"Monitoring insert rate every {interval} second(s). Press Ctrl+C to stop and see summary."
    )

    initial_doc_count_script_start = 0
    try:
        initial_doc_count_script_start = collection.count_documents({})
    except Exception as e:
        print(
            f"Error getting initial document count: {e}. Assuming 0.", file=sys.stderr
        )

    script_start_time_monotonic = time.monotonic()  # For total script duration
    script_start_wall_time = datetime.datetime.now(datetime.timezone.utc)

    previous_doc_count = initial_doc_count_script_start
    previous_time_monotonic = script_start_time_monotonic
    last_display_len = 0

    data_insertion_active = False
    active_insertion_start_time_monotonic = None
    doc_count_at_active_start = 0

    try:
        while keep_running:
            time.sleep(interval)
            if not keep_running:
                break

            current_time_monotonic = time.monotonic()
            current_doc_count = 0
            try:
                current_doc_count = collection.count_documents({})
            except Exception as e:
                sys.stdout.write("\r" + " " * last_display_len + "\r")
                print(
                    f"Error getting document count: {e}. Retrying...", file=sys.stderr
                )
                last_display_len = 0
                continue

            delta_docs_poll = current_doc_count - previous_doc_count
            delta_time_poll = current_time_monotonic - previous_time_monotonic

            current_rate_docs_sec = 0.0
            if delta_time_poll > 0:
                current_rate_docs_sec = delta_docs_poll / delta_time_poll

            if (
                not data_insertion_active and delta_docs_poll > 0
            ):  # First sign of inserts
                data_insertion_active = True
                # Start timing for overall average from the point *before* this first batch
                active_insertion_start_time_monotonic = previous_time_monotonic
                doc_count_at_active_start = previous_doc_count
                print(
                    "\nData insertion detected. Overall average calculation started."
                )  # Newline to not overwrite this
                last_display_len = 0  # Reset for next display line

            overall_avg_docs_sec = 0.0
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
                    overall_avg_docs_sec = (
                        total_docs_inserted_active_period / active_period_elapsed_time
                    )

            total_script_elapsed_time = (
                current_time_monotonic - script_start_time_monotonic
            )
            total_docs_inserted_script_start = (
                current_doc_count - initial_doc_count_script_start
            )

            display_string = (
                f"Time Elapsed (Total): {total_script_elapsed_time:7.2f}s | "
                f"Total Docs: {current_doc_count:,} ({'+' if total_docs_inserted_script_start >=0 else ''}{total_docs_inserted_script_start:,} since start) | "
                f"Current Rate: {current_rate_docs_sec:7.2f} docs/s | "
                f"Overall Avg (Active): {overall_avg_docs_sec:7.2f} docs/s  "
            )

            sys.stdout.write("\r" + " " * last_display_len + "\r")
            sys.stdout.write(display_string)
            sys.stdout.flush()
            last_display_len = len(display_string)

            previous_doc_count = current_doc_count
            previous_time_monotonic = current_time_monotonic

    except KeyboardInterrupt:
        keep_running = False
        print("\nKeyboardInterrupt. Finalizing...")
    except Exception as e:
        sys.stdout.write("\n")
        print(f"Error during monitoring: {e}", file=sys.stderr)
    finally:
        if not final_report_generated:
            sys.stdout.write("\n" + "-" * 20 + " FINAL SUMMARY " + "-" * 20 + "\n")
            final_time_monotonic = time.monotonic()
            final_doc_count = initial_doc_count_script_start
            try:
                final_doc_count = collection.count_documents({})
            except Exception as e:
                print(
                    f"Error getting final document count: {e}. Using last known.",
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

            if data_insertion_active:
                active_period_duration_final = (
                    final_time_monotonic - active_insertion_start_time_monotonic
                )
                docs_inserted_active_final = final_doc_count - doc_count_at_active_start
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

        if "client" in locals() and client:
            try:
                client.close()
                print("MongoDB connection closed.")
            except Exception as e:
                print(f"Error closing MongoDB connection: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Monitor MongoDB insertion rate by periodically counting documents.",
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

    args = parser.parse_args()
    if args.interval <= 0:
        print("Error: --interval must be positive.", file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    monitor_mongodb_inserts(args.uri, args.db, args.collection, args.interval)
    print("Monitoring script finished.")
