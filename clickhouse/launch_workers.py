import argparse
import shlex  # For robust argument splitting
import signal
import subprocess
import sys
import time
from pathlib import Path

# --- Global list to hold launched worker processes ---
running_processes = []


def signal_handler(sig, frame):
    """Gracefully terminate all child worker processes on SIGINT (Ctrl+C)."""
    print("\nSIGINT received! Terminating all running worker processes...")
    # Iterate over a copy of the list to avoid issues with modification
    for p_info in list(running_processes):
        process = p_info["process"]
        worker_id = p_info["id"]
        if process.poll() is None:  # Check if process is still running
            print(f"  Terminating worker {worker_id} (PID: {process.pid})...")
            try:
                process.terminate()  # Sends SIGTERM, allows for graceful shutdown
            except ProcessLookupError:
                print(f"  Worker {worker_id} (PID: {process.pid}) already terminated.")
            except Exception as e:
                print(f"  Error terminating worker {worker_id}: {e}")

    # Give processes a moment to terminate gracefully
    time.sleep(2)
    for p_info in running_processes:
        if p_info["process"].poll() is None:  # If still running
            print(f"  Worker {p_info['id']} did not terminate, sending SIGKILL...")
            p_info["process"].kill()

    print("All workers signaled to terminate. Exiting launcher.")
    sys.exit(0)


def launch_workers(args):
    """
    Launches and manages a set of worker processes based on the selected mode.
    """
    global running_processes

    # Register the signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    print(f"--- ClickHouse Benchmark Launcher ---")
    print(f"Mode: {args.mode}")
    print(f"Location: {args.location}")
    print(f"Number of Concurrent Workers: {args.num_workers}")
    print(f"Worker Batch Size: {args.batch_size}")

    # --- Validate Mode and Location ---
    if args.mode == "strided":
        if not args.location.is_file():
            print(
                f"Error: In 'strided' mode, --location must be a single file.",
                file=sys.stderr,
            )
            sys.exit(1)
    elif args.mode == "presplit":
        if not args.location.is_dir():
            print(
                f"Error: In 'presplit' mode, --location must be a directory.",
                file=sys.stderr,
            )
            sys.exit(1)

    # --- Build the base command for the worker script ---
    # Assumes 'inserter_clickhouse.py' is in the same directory as this script
    worker_script_path = Path(__file__).parent / "inserter_clickhouse.py"
    if not worker_script_path.is_file():
        print(
            f"Error: Worker script 'inserter_clickhouse.py' not found in the same directory.",
            file=sys.stderr,
        )
        sys.exit(1)

    base_worker_command = (
        f"{sys.executable} {worker_script_path} "
        f"--host {args.host} --port {args.port} --user {args.user} --password '{args.password}' "
        f"--db {args.db} --table {args.table} --batch_size {args.batch_size}"
    )

    # --- Execute based on mode ---
    if args.mode == "strided":
        print(
            f"\nLaunching {args.num_workers} workers in STRIDED mode on file '{args.location.name}'..."
        )
        for i in range(args.num_workers):
            # Construct the full command for this specific worker
            worker_command_str = (
                f"{base_worker_command} "
                f"--tsv_file {args.location} "
                f"--worker_num {i} "
                f"--total_workers {args.num_workers}"
            )
            command_list = shlex.split(worker_command_str)
            print(f"  Launching Worker {i+1}/{args.num_workers}...")
            try:
                process = subprocess.Popen(command_list)
                running_processes.append({"process": process, "id": f"Worker-{i+1}"})
            except Exception as e:
                print(f"\nERROR launching worker {i+1}: {e}", file=sys.stderr)
                signal_handler(
                    None, None
                )  # Trigger cleanup of any already started workers

    elif args.mode == "presplit":
        files_to_process = sorted(
            [f for f in args.location.glob("*.tsv") if f.is_file()]
        )
        if not files_to_process:
            print(f"No .tsv files found in directory: {args.location}")
            return

        print(
            f"\nFound {len(files_to_process)} files to process. Will run with up to {args.num_workers} workers concurrently."
        )
        task_queue = list(files_to_process)

        while task_queue or running_processes:
            # Clean up completed workers from the running list
            running_processes = [
                p for p in running_processes if p["process"].poll() is None
            ]

            # Launch new workers if there's capacity and tasks remaining
            while len(running_processes) < args.num_workers and task_queue:
                file_to_process = task_queue.pop(0)

                worker_command_str = (
                    f"{base_worker_command} --tsv_file {file_to_process}"
                )
                command_list = shlex.split(worker_command_str)

                print(f"Launching worker for '{file_to_process.name}'...")
                try:
                    process = subprocess.Popen(command_list)
                    running_processes.append(
                        {"process": process, "id": file_to_process.name}
                    )
                    print(
                        f"  > {len(running_processes)} active, {len(task_queue)} files remaining."
                    )
                except Exception as e:
                    print(
                        f"\nERROR launching worker for {file_to_process.name}: {e}",
                        file=sys.stderr,
                    )
                    signal_handler(None, None)

            if not task_queue and not running_processes:
                break  # All done

            time.sleep(2)  # Brief pause to prevent a busy-wait loop

    # --- Wait for all processes to complete ---
    print(
        f"\n--- All {len(running_processes)} workers launched. Orchestrator is now waiting. ---"
    )
    print("--- Press Ctrl+C to terminate all workers and exit. ---")

    failed_workers = []
    for p_info in running_processes:
        process, worker_id = p_info["process"], p_info["id"]
        exit_code = process.wait()
        if exit_code != 0:
            failed_workers.append({"id": worker_id, "exit_code": exit_code})
            print(
                f"Worker '{worker_id}' (PID: {process.pid}) finished with non-zero exit code: {exit_code}"
            )
        else:
            print(f"Worker '{worker_id}' (PID: {process.pid}) finished successfully.")

    print("\n--- All work has completed. ---")
    if failed_workers:
        print("Summary of failed workers:")
        for failure in failed_workers:
            print(
                f"  Worker '{failure['id']}' failed with exit code {failure['exit_code']}"
            )
    else:
        print("All workers completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Launch and manage parallel ClickHouse inserter workers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Mode and Location
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["strided", "presplit"],
        help="The processing mode. 'strided': all workers read one large file. "
        "'presplit': each worker gets a file from a directory.",
    )
    parser.add_argument(
        "--location",
        type=Path,
        required=True,
        help="Path to the source data. A single TSV file for 'strided' mode, "
        "or a directory of TSV files for 'presplit' mode.",
    )
    # Worker Config
    parser.add_argument(
        "--num_workers",
        type=int,
        required=True,
        help="The number of parallel worker processes to launch.",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=50000,
        help="The internal batch size (rows) for each worker script.",
    )
    # ClickHouse Connection
    parser.add_argument("--host", default="localhost", help="ClickHouse host.")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port.")
    parser.add_argument("--user", default="default", help="ClickHouse user.")
    parser.add_argument("--password", default="", help="ClickHouse password.")
    parser.add_argument("--db", required=True, help="ClickHouse Database name.")
    parser.add_argument("--table", required=True, help="ClickHouse Table name.")

    args = parser.parse_args()

    if args.num_workers < 1:
        print("Error: --num_workers must be at least 1.", file=sys.stderr)
        sys.exit(1)

    launch_workers(args)
