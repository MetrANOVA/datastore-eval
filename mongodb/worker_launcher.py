# launch_workers.py
import argparse
import shlex  # For robust argument splitting
import signal
import subprocess
import sys
import time

# --- Global list to hold launched worker processes ---
running_processes = []


def signal_handler(sig, frame):
    """Gracefully terminate all child worker processes on SIGINT (Ctrl+C)."""
    print("\nSIGINT received! Terminating worker processes...")
    for p_info in running_processes:
        process = p_info["process"]
        worker_num = p_info["worker_num"]
        print(f"  Terminating worker {worker_num} (PID: {process.pid})...")
        try:
            process.terminate()  # Sends SIGTERM, allowing for graceful shutdown
        except ProcessLookupError:
            print(f"  Worker {worker_num} (PID: {process.pid}) already terminated.")
        except Exception as e:
            print(f"  Error terminating worker {worker_num}: {e}")

    # Optional: Wait a moment and then force kill if any are still alive
    time.sleep(1)
    for p_info in running_processes:
        if p_info["process"].poll() is None:  # If still running
            print(
                f"  Worker {p_info['worker_num']} did not terminate gracefully, sending SIGKILL..."
            )
            p_info["process"].kill()

    print("All workers signaled to terminate. Exiting orchestrator.")
    sys.exit(0)


def launch_workers(num_workers: int, worker_command_template: str):
    """
    Launches and manages a set of worker processes.

    Args:
        num_workers: The total number of worker processes to launch.
        worker_command_template: The command string to execute for each worker,
                                 containing placeholders {worker_num} and {total_workers}.
    """
    global running_processes

    # Register the signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    print(f"--- Launching {num_workers} Worker Processes ---")

    for i in range(num_workers):
        # Substitute placeholders with actual values for this worker
        command_str = worker_command_template.format(
            worker_num=i, total_workers=num_workers
        )

        # Use shlex.split for robust handling of quoted arguments
        command_list = shlex.split(command_str)

        print(f"Launching Worker {i+1}/{num_workers}: {' '.join(command_list)}")

        try:
            # Popen starts the process in the background and continues
            # stdout and stderr are not captured, they will appear in the console
            process = subprocess.Popen(command_list)
            running_processes.append({"process": process, "worker_num": i})
        except FileNotFoundError:
            print(
                f"\nError: Command not found. Ensure '{command_list[0]}' is executable and in your PATH.",
                file=sys.stderr,
            )
            print(
                "Terminating already launched workers before exiting.", file=sys.stderr
            )
            # Trigger cleanup for any processes that were already launched
            signal_handler(None, None)
        except Exception as e:
            print(f"\nError launching worker {i}: {e}", file=sys.stderr)
            signal_handler(None, None)

    print(
        f"\n--- All {len(running_processes)} workers launched. Orchestrator is now waiting. ---"
    )
    print("--- Press Ctrl+C to terminate all workers and exit. ---")

    # Wait for all processes to complete and check for failures
    failed_workers = []
    for p_info in running_processes:
        process = p_info["process"]
        worker_num = p_info["worker_num"]

        # wait() blocks until the process finishes
        exit_code = process.wait()

        if exit_code != 0:
            failed_workers.append({"worker_num": worker_num, "exit_code": exit_code})
            print(
                f"Worker {worker_num} (PID: {process.pid}) finished with a non-zero exit code: {exit_code}"
            )
        else:
            print(f"Worker {worker_num} (PID: {process.pid}) finished successfully.")

    print("\n--- All workers have completed. ---")
    if failed_workers:
        print("Summary of failed workers:")
        for failure in failed_workers:
            print(
                f"  Worker {failure['worker_num']} failed with exit code {failure['exit_code']}"
            )
    else:
        print("All workers completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Launch and manage a set of parallel worker scripts.",
        formatter_class=argparse.RawTextHelpFormatter,  # Allows for better formatting of help text
    )
    parser.add_argument(
        "-n",
        "--num_workers",
        type=int,
        required=True,
        help="The total number of parallel worker processes to launch.",
    )
    parser.add_argument(
        "-c",
        "--worker_command",
        type=str,
        required=True,
        help="The command string to execute for each worker.\n"
        "Use placeholders {worker_num} and {total_workers}.\n"
        "Example: 'python my_script.py --id {worker_num} --total {total_workers}'\n"
        "IMPORTANT: Enclose the entire command in single or double quotes.",
    )

    args = parser.parse_args()

    if args.num_workers < 1:
        print("Error: --num_workers must be at least 1.", file=sys.stderr)
        sys.exit(1)

    launch_workers(args.num_workers, args.worker_command)
