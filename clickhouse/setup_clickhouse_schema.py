import argparse
import sys

import clickhouse_connect


def setup_clickhouse(host, port, user, password, db_name, table_name):
    """
    Connects to ClickHouse, creates the database and the time series table
    based on the provided metadata structure.

    Args:
        host (str): ClickHouse host.
        port (int): ClickHouse HTTP port (usually 8123).
        user (str): ClickHouse user.
        password (str): ClickHouse password.
        db_name (str): Database name to create/use.
        table_name (str): Table name to create.
    """
    print(f"Attempting to connect to ClickHouse at {host}:{port}...")
    try:
        # Connect without specifying database initially to ensure CREATE DATABASE works
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            user=user,
            password=password,
            connect_timeout=10,
            send_receive_timeout=30,
        )
        client.ping()  # Verify connection
        print("ClickHouse connection successful.")
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        # --- Create Database ---
        print(f"Creating database '{db_name}' if it doesn't exist...")
        client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' is ready.")
        # Reconnect or set database context
        client.database = db_name

        # --- Define Columns based on Sample ---
        # Required base fields + measurements
        columns = [
            "`timestamp` DateTime64(3, 'UTC')",  # Millisecond precision, stored as UTC
            "`node` String",  # REQUIRED based on sample
            "`intf` String",  # REQUIRED based on sample (interface name)
            "`input` Float64",  # Measurement
            "`output` Float64",  # Measurement
        ]

        # Metadata fields from sample - make them Nullable Strings
        # Using backticks `` for keys with dots or special characters
        metadata_columns = [
            "`alternate_intf` Nullable(String)",
            "`circuit.carrier` Nullable(String)",
            "`circuit.carrier_id` Nullable(String)",  # Keeping IDs as String for flexibility
            "`circuit.carrier_type` Nullable(String)",
            "`circuit.circuit_id` Nullable(String)",
            "`circuit.customer` Nullable(String)",
            "`circuit.customer_id` Nullable(String)",
            "`circuit.customer_type` Nullable(String)",
            "`circuit.description` Nullable(String)",
            "`circuit.name` Nullable(String)",
            "`circuit.role` Nullable(String)",  # Storing the list-like string as String
            "`circuit.type` Nullable(String)",
            "`contracted_bandwidth` Nullable(String)",  # Keeping as String due to '0' example
            "`description` Nullable(String)",
            "`interface_address` Nullable(String)",  # Storing '[]' as String
            "`interface_id` Nullable(String)",
            "`network` Nullable(String)",
            # Skipping node, intf, input, output as they are defined above
            "`node_id` Nullable(String)",
            "`node_management_address` Nullable(String)",  # Could be IP type, but String is safer
            "`node_role` Nullable(String)",
            "`node_type` Nullable(String)",
            "`parent_interface` Nullable(String)",
            "`pop.id` Nullable(String)",
            "`pop.name` Nullable(String)",
            "`pop.type` Nullable(String)",
            "`service.description` Nullable(String)",
            "`service.direction` Nullable(String)",
            "`service.entity` Nullable(String)",
            "`service.entity_id` Nullable(String)",
            "`service.entity_roles` Nullable(String)",  # Storing '[]' as String
            "`service.entity_type` Nullable(String)",
            "`service.name` Nullable(String)",
            "`service.service_id` Nullable(String)",
            "`service.type` Nullable(String)",
            "`type` Nullable(String)",  # Matches the last field in sample's metadata
        ]
        columns.extend(metadata_columns)

        column_definitions = ",\n    ".join(columns)

        # --- Create Table Statement ---
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
            {column_definitions}
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)       -- Partition by month
        ORDER BY (node, intf, timestamp)       -- ORDER BY required fields + timestamp
        """

        print(f"Creating table '{db_name}.{table_name}' if it doesn't exist...")
        print("--- Schema ---")
        print(create_table_sql)
        print("--------------")

        client.command(create_table_sql)
        print(f"Table '{db_name}.{table_name}' is ready.")

    except Exception as e:
        print(f"An error occurred during setup: {e}", file=sys.stderr)
        # Attempt to get more details from ClickHouse errors if possible
        if hasattr(e, "message") and e.message:
            print(f"ClickHouse Error Details: {e.message}", file=sys.stderr)
        sys.exit(1)
    finally:
        if "client" in locals() and client.is_connected:
            client.close()
            print("ClickHouse connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create ClickHouse Database and Time Series Table based on specific schema."
    )
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
    parser.add_argument("--db", required=True, help="Database name to create/use")
    parser.add_argument("--table", required=True, help="Table name to create")

    args = parser.parse_args()
    setup_clickhouse(
        args.host, args.port, args.user, args.password, args.db, args.table
    )
