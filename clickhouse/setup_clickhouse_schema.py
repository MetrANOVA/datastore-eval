import argparse
import sys
from pathlib import Path

import clickhouse_connect


def setup_clickhouse_from_file(
    host: str,
    port: int,
    user: str,
    password: str,
    db_name: str,
    sql_file_path: Path,
):
    """
    Connects to ClickHouse, ensures the database exists, and executes
    the SQL commands (e.g., CREATE TABLE) from a specified file.

    Args:
        host: ClickHouse host.
        port: ClickHouse HTTP port.
        user: ClickHouse user.
        password: ClickHouse password.
        db_name: Database name to ensure exists.
        sql_file_path: Path to the SQL file containing schema definitions.
    """
    print(f"Attempting to connect to ClickHouse at {host}:{port}...")
    try:
        # Connect without specifying database initially
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            user=user,
            password=password,
            connect_timeout=10,
            send_receive_timeout=60,
        )
        client.ping()
        print("ClickHouse connection successful.")
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        # --- Ensure Database Exists ---
        print(f"Ensuring database '{db_name}' exists...")
        client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")  # Quote db name
        print(f"Database '{db_name}' is ready.")
        # Set database context for subsequent commands if needed, though
        # the SQL file should ideally contain qualified table names (db.table)
        # client.database = db_name

        # --- Read SQL Schema File ---
        print(f"Reading schema definition from: {sql_file_path}")
        try:
            with open(sql_file_path, "r", encoding="utf-8") as f_sql:
                sql_command = f_sql.read()
            if not sql_command.strip():
                print(f"Error: SQL file '{sql_file_path}' is empty.", file=sys.stderr)
                sys.exit(1)
            print("SQL schema file read successfully.")
        except FileNotFoundError:
            print(f"Error: SQL file not found at '{sql_file_path}'", file=sys.stderr)
            sys.exit(1)
        except IOError as e:
            print(f"Error reading SQL file '{sql_file_path}': {e}", file=sys.stderr)
            sys.exit(1)

        # --- Execute SQL Command ---
        # This will likely contain the CREATE TABLE statement
        print(f"Executing schema setup SQL from {sql_file_path.name}...")
        # client.command() can execute multi-statement SQL if separated by semicolons
        # and appropriate server settings are enabled, but typically expect one main command here.
        client.command(sql_command)
        print(f"Successfully executed schema setup from {sql_file_path.name}.")

    except Exception as e:
        print(f"An error occurred during setup: {e}", file=sys.stderr)
        if hasattr(e, "message") and e.message:
            print(f"ClickHouse Error Details: {e.message}", file=sys.stderr)
        sys.exit(1)
    finally:
        # Corrected disconnect logic
        if "client" in locals() and client:
            try:
                client.close()
                print("ClickHouse connection closed.")
            except Exception as e:
                print(f"Error during ClickHouse connection close: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ensure ClickHouse DB exists and execute schema setup from SQL file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port")
    parser.add_argument("--user", default="default", help="ClickHouse user")
    parser.add_argument("--password", default="", help="ClickHouse password")
    parser.add_argument("--db", required=True, help="Database name to ensure exists.")
    parser.add_argument(
        "--sql-file",
        required=True,
        type=Path,
        help="Path to the SQL file containing the CREATE TABLE statement(s).",
    )

    args = parser.parse_args()
    setup_clickhouse_from_file(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        db_name=args.db,
        sql_file_path=args.sql_file,
    )
