# db_side_monitor_ch.py
import argparse
import csv
import datetime
import signal
import sys
import time
from pathlib import Path

import clickhouse_connect  # Using ClickHouse library

# --- Global flag for graceful exit ---
keep_running = True
final_report_generated = False


def signal_handler(sig, frame):
    """Handles SIGINT (Ctrl+C) for graceful shutdown."""
    global keep_running
    if keep_running:
        sys.stdout.write(
            "\r" + " " * getattr(sys.stdout, "_last_display_len", 0) + "\r"
        )
        print("\nSIGINT received. Finalizing report and shutting down...")
        sys.stdout.flush()
        keep_running = False
    else:
        print("Forcing exit...")
        sys.exit(1)


def _clear_previous_line(last_len):
    """Helper to clear the previous line in stdout."""
    sys.stdout.write("\r" + " " * last_len + "\r")
    sys.stdout.flush()


def monitor_clickhouse_inserts(
    host: str,
    port: int,
    user: str,
    password: str,
    db_name: str,
    table_name: str,
    interval: float,
    output_csv_path: Path = None,
    idle_timeout_polls: int = 5,
):
    global keep_running, final_report_generated
    final_report_generated = False
    sys.stdout._last_display_len = 0

    csv_writer = None
    csv_file_handle = None

    print(f"Attempting to connect to ClickHouse at {host}:{port}...")
    try:
        # --- ClickHouse Connection ---
        client = clickhouse_connect.get_client(
            host=host, port=port, user=user, password=password, database=db_name
        )
        client.ping()
        print(f"Successfully connected to DB: '{db_name}', Table: '{table_name}'.")
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
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
                "current_total_rows",
                "delta_rows_interval",
                "delta_time_interval",
                "current_rate_rows_sec",
                "total_rows_active_period",
                "overall_avg_rows_sec_active",
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

    initial_row_count_script_start = 0
    try:
        # --- ClickHouse Row Count ---
        initial_row_count_script_start = client.query(
            f"SELECT count() FROM {table_name}"
        ).first_row[0]
    except Exception as e:
        print(f"Error getting initial row count: {e}. Assuming 0.", file=sys.stderr)

    script_start_time_monotonic = time.monotonic()
    script_start_wall_time = datetime.datetime.now(datetime.timezone.utc)
    previous_row_count = initial_row_count_script_start
    previous_time_monotonic = script_start_time_monotonic

    data_insertion_active = False
    active_insertion_start_time_monotonic = None
    row_count_at_active_start = 0
    consecutive_idle_polls = 0

    try:
        while keep_running:
            time.sleep(interval)
            if not keep_running:
                break

            current_time_monotonic = time.monotonic()
            measurement_wall_time_utc = datetime.datetime.now(datetime.timezone.utc)
            current_row_count = 0
            try:
                # --- ClickHouse Row Count ---
                current_row_count = client.query(
                    f"SELECT count() FROM {table_name}"
                ).first_row[0]
            except Exception as e:
                _clear_previous_line(sys.stdout._last_display_len)
                print(f"Error getting row count: {e}. Retrying...", file=sys.stderr)
                sys.stdout._last_display_len = 0
                continue

            delta_rows_poll = current_row_count - previous_row_count
            delta_time_poll = current_time_monotonic - previous_time_monotonic
            current_rate_rows_sec = 0.0
            if delta_time_poll > 0:
                current_rate_rows_sec = delta_rows_poll / delta_time_poll

            if not data_insertion_active and delta_rows_poll > 0:
                data_insertion_active = True
                active_insertion_start_time_monotonic = previous_time_monotonic
                row_count_at_active_start = previous_row_count
                _clear_previous_line(sys.stdout._last_display_len)
                print(
                    f"INFO: Data insertion detected at {measurement_wall_time_utc.isoformat()}. Overall average calculation for active period started."
                )
                sys.stdout._last_display_len = 0

            overall_avg_rows_sec_active = 0.0
            total_rows_inserted_active_period = 0
            active_period_elapsed_time = 0.0

            if data_insertion_active:
                active_period_elapsed_time = (
                    current_time_monotonic - active_insertion_start_time_monotonic
                )
                total_rows_inserted_active_period = (
                    current_row_count - row_count_at_active_start
                )
                if active_period_elapsed_time > 0:
                    overall_avg_rows_sec_active = (
                        total_rows_inserted_active_period / active_period_elapsed_time
                    )

                # Auto-exit logic
                if delta_rows_poll == 0:
                    consecutive_idle_polls += 1
                else:
                    consecutive_idle_polls = 0

                if (
                    idle_timeout_polls > 0
                    and consecutive_idle_polls >= idle_timeout_polls
                ):
                    _clear_previous_line(sys.stdout._last_display_len)
                    print(
                        f"INFO: No new rows inserted for {consecutive_idle_polls} poll(s). Test assumed complete."
                    )
                    sys.stdout._last_display_len = 0
                    keep_running = False

            total_script_elapsed_time = (
                current_time_monotonic - script_start_time_monotonic
            )
            total_rows_inserted_script_start = (
                current_row_count - initial_row_count_script_start
            )

            display_string = (
                f"Time Elapsed: {total_script_elapsed_time:7.2f}s | "
                f"Total Rows: {current_row_count:,} ({'+' if total_rows_inserted_script_start >=0 else ''}{total_rows_inserted_script_start:,}) | "
                f"Rate (current): {current_rate_rows_sec:7.2f} rows/s | "
                f"Avg Rate (active): {overall_avg_rows_sec_active:7.2f} rows/s | "
                f"Idle Polls: {consecutive_idle_polls if data_insertion_active and idle_timeout_polls > 0 else 'N/A'}  "
            )

            _clear_previous_line(sys.stdout._last_display_len)
            sys.stdout.write(display_string)
            sys.stdout.flush()
            sys.stdout._last_display_len = len(display_string)

            if csv_writer:
                csv_row = [  # Changed variable names to match header
                    measurement_wall_time_utc.isoformat(),
                    round(total_script_elapsed_time, 3),
                    (
                        round(active_period_elapsed_time, 3)
                        if data_insertion_active
                        else 0.0
                    ),
                    current_row_count,
                    delta_rows_poll,
                    round(delta_time_poll, 3),
                    round(current_rate_rows_sec, 3),
                    total_rows_inserted_active_period if data_insertion_active else 0,
                    (
                        round(overall_avg_rows_sec_active, 3)
                        if data_insertion_active
                        else 0.0
                    ),
                ]
                try:
                    csv_writer.writerow(csv_row)
                    csv_file_handle.flush()
                except Exception as e:
                    _clear_previous_line(sys.stdout._last_display_len)
                    print(f"\nError writing to CSV: {e}", file=sys.stderr)
                    sys.stdout._last_display_len = 0

            previous_row_count = current_row_count
            previous_time_monotonic = current_time_monotonic

    except KeyboardInterrupt:
        if keep_running:
            _clear_previous_line(sys.stdout._last_display_len)
            print("\nKeyboardInterrupt. Finalizing...")
            keep_running = False
    except Exception as e:
        _clear_previous_line(sys.stdout._last_display_len)
        print(f"\nError during monitoring: {e}", file=sys.stderr)
    finally:
        if sys.stdout._last_display_len > 0:
            _clear_previous_line(sys.stdout._last_display_len)
        if not final_report_generated:
            print("\n" + "-" * 20 + " FINAL SUMMARY " + "-" * 20)
            final_time_monotonic = time.monotonic()
            final_row_count = initial_row_count_script_start
            try:
                final_row_count = client.query(
                    f"SELECT count() FROM {table_name}"
                ).first_row[0]
            except Exception as e:
                print(
                    f"Error getting final row count: {e}. Using last known.",
                    file=sys.stderr,
                )
                final_row_count = previous_row_count

            total_script_duration_final = (
                final_time_monotonic - script_start_time_monotonic
            )
            total_rows_inserted_script_final = (
                final_row_count - initial_row_count_script_start
            )

            print(f"Monitoring Started (UTC): {script_start_wall_time.isoformat()}")
            print(
                f"Monitoring Ended   (UTC): {datetime.datetime.now(datetime.timezone.utc).isoformat()}"
            )
            print(
                f"Total Monitoring Duration: {total_script_duration_final:.2f} seconds"
            )
            print(f"Initial Row Count: {initial_row_count_script_start:,}")
            print(f"Final Row Count:   {final_row_count:,}")
            print(
                f"Total Rows Inserted During Monitoring Period: {total_rows_inserted_script_final:,}"
            )

            if data_insertion_active:
                active_period_duration_final = final_time_monotonic - (
                    active_insertion_start_time_monotonic
                    if active_insertion_start_time_monotonic
                    else script_start_time_monotonic
                )
                rows_inserted_active_final = final_row_count - (
                    row_count_at_active_start
                    if active_insertion_start_time_monotonic
                    else initial_row_count_script_start
                )
                overall_avg_rows_sec_final_active = 0.0
                if active_period_duration_final > 0 and rows_inserted_active_final > 0:
                    overall_avg_rows_sec_final_active = (
                        rows_inserted_active_final / active_period_duration_final
                    )
                print(
                    f"Duration of Active Insertion: {active_period_duration_final:.2f} seconds"
                )
                print(
                    f"Rows Inserted During Active Period: {rows_inserted_active_final:,}"
                )
                print(
                    f"Overall Average Insertion Rate (During Active Period): {overall_avg_rows_sec_final_active:.2f} rows/s"
                )
            else:
                print("No data insertion was detected during the monitoring period.")
            print("-" * 55)
            final_report_generated = True

        if csv_file_handle:
            try:
                csv_file_handle.close()
                print(f"Measurement CSV '{output_csv_path}' closed.")
            except:
                pass
        if "client" in locals() and client:
            try:
                client.close()
                print("ClickHouse connection closed.")
            except:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Monitor ClickHouse insertion rate and log to CSV."
    )
    parser.add_argument("--host", default="localhost", help="ClickHouse host.")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port.")
    parser.add_argument("--user", default="default", help="ClickHouse user.")
    parser.add_argument("--password", default="", help="ClickHouse password.")
    parser.add_argument("--db", required=True, help="ClickHouse Database name.")
    parser.add_argument(
        "--table", required=True, help="ClickHouse Table name to monitor."
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
        help="Polls with no new rows before auto-exiting (0 to disable).",
    )

    args = parser.parse_args()
    if args.interval <= 0:
        print("Error: --interval must be positive.", file=sys.stderr)
        sys.exit(1)
    if args.idle_timeout_polls < 0:
        print("Error: --idle_timeout_polls cannot be negative.", file=sys.stderr)
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    monitor_clickhouse_inserts(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        db_name=args.db,
        table_name=args.table,
        interval=args.interval,
        output_csv_path=args.output_csv,
        idle_timeout_polls=args.idle_timeout_polls,
    )
    print("Monitoring script finished.")
