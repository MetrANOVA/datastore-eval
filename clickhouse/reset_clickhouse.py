import argparse
import sys

import clickhouse_connect


def reset_clickhouse_table(host, port, user, password, db_name, table_name):
    """
    Connects to ClickHouse and truncates the specified table.

    Args:
        host (str): ClickHouse host.
        port (int): ClickHouse HTTP port.
        user (str): ClickHouse user.
        password (str): ClickHouse password.
        db_name (str): Database where the table exists.
        table_name (str): Table name to truncate.
    """
    print(f"Attempting to connect to ClickHouse at {host}:{port}...")
    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, user=user, password=password, database=db_name
        )
        client.ping()
        print("ClickHouse connection successful.")
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print(f"Attempting to TRUNCATE table '{db_name}.{table_name}'...")
        # TRUNCATE is generally fast for MergeTree tables
        client.command(f"TRUNCATE TABLE IF EXISTS {db_name}.{table_name}")
        print(
            f"Table '{db_name}.{table_name}' truncated successfully (or did not exist)."
        )

    except Exception as e:
        print(f"An error occurred during truncate: {e}", file=sys.stderr)
        # You might want more specific error handling here
    finally:
        if "client" in locals() and client.is_connected:
            client.close()
            print("ClickHouse connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Truncate a ClickHouse Table.")
    parser.add_argument(
        "--host", default="localhost", help="ClickHouse host (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=8123, help="ClickHouse HTTP port (default: 8123)"
    )
    parser.add_argument(
        "--user", default="default", help="ClickHouse user (default: default)"
    )
    parser.add_argument(
        "--password", default="", help="ClickHouse password (default: empty)"
    )
    parser.add_argument(
        "--db", required=True, help="Database name containing the table"
    )
    parser.add_argument("--table", required=True, help="Table name to truncate")

    args = parser.parse_args()

    reset_clickhouse_table(
        args.host, args.port, args.user, args.password, args.db, args.table
    )
