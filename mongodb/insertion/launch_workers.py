# launch_workers.py
import argparse
import shlex
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
                process.terminate()  # Sends SIGTERM
            except ProcessLookupError:
                pass
            except Exception as e:
                print(f"  Error terminating worker {worker_id}: {e}")

    time.sleep(2)
    for p_info in running_processes:
        if p_info["process"].poll() is None:  # If still running
            print(f"  Worker {p_info['id']} did not terminate, sending SIGKILL...")
            p_info["process"].kill()

    print("All workers signaled to terminate. Exiting launcher.")
    sys.exit(0)


def launch_workers(args):
    """
    Manages a pool of worker processes to run the inserter script on a set of files.
    """
    global running_processes

    signal.signal(signal.SIGINT, signal_handler)

    print(f"--- Parallel Benchmark Launcher ---")
    print(f"Input Directory: {args.input_dir}")
    print(f"Max Concurrent Workers: {args.num_workers}")
    print(f"Worker Batch Size: {args.batch_size}")

    if not args.input_dir.is_dir():
        print(
            f"Error: Input directory not found at '{args.input_dir}'", file=sys.stderr
        )
        sys.exit(1)

    # Assumes 'benchmark.py' is in the same directory as this script
    worker_script_path = Path(__file__).parent / "benchmark.py"
    if not worker_script_path.is_file():
        print(
            f"Error: Worker script 'benchmark.py' not found in the same directory.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Base command for the worker script
    base_worker_command = (
        f"{sys.executable} {worker_script_path} "
        f"--uri '{args.uri}' --db {args.db} --collection {args.collection} "
        f"--batch_size {args.batch_size}"
    )

    # Find all pre-computed .jsonl files in the target directory
    files_to_process = sorted(
        [f for f in args.input_dir.glob("*.jsonl") if f.is_file()]
    )
    if not files_to_process:
        print(
            f"Error: No .jsonl files found in directory: {args.input_dir}",
            file=sys.stderr,
        )
        return

    print(
        f"\nFound {len(files_to_process)} pre-computed JSONL files. Will run with up to {args.num_workers} workers concurrently."
    )
    task_queue = list(files_to_process)

    try:
        while task_queue or running_processes:
            # Clean up completed workers from the running list
            for p_info in list(running_processes):
                if p_info["process"].poll() is not None:
                    running_processes.remove(p_info)
                    if p_info["process"].returncode == 0:
                        print(f"Worker for '{p_info['id']}' completed successfully.")
                    else:
                        print(
                            f"WARNING: Worker for '{p_info['id']}' finished with non-zero exit code: {p_info['process'].returncode}.",
                            file=sys.stderr,
                        )

            # Launch new workers if there's capacity and tasks remaining
            while len(running_processes) < args.num_workers and task_queue:
                file_to_process = task_queue.pop(0)

                # The worker script's file argument is --input_file
                worker_command_str = (
                    f"{base_worker_command} --input_file {file_to_process}"
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
                break

            time.sleep(2)
    except KeyboardInterrupt:
        signal_handler(None, None)

    print(f"\n--- All work has completed. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Launch and manage a pool of workers to ingest pre-computed data files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input_dir",
        type=Path,
        required=True,
        help="Directory containing the pre-computed .jsonl data files.",
    )
    parser.add_argument(
        "--num_workers",
        type=int,
        required=True,
        help="The number of worker processes to run concurrently.",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=5000,
        help="The internal batch size (documents) for each worker script.",
    )
    # MongoDB Connection
    parser.add_argument(
        "--uri", default="mongodb://localhost:27017/", help="MongoDB connection URI."
    )
    parser.add_argument("--db", required=True, help="MongoDB Database name.")
    parser.add_argument("--collection", required=True, help="MongoDB Collection name.")

    args = parser.parse_args()
    if args.num_workers < 1:
        print("Error: --num_workers must be at least 1.", file=sys.stderr)
        sys.exit(1)

    launch_workers(args)
