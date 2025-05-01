import argparse
import csv
import sys
from pathlib import Path


def generate_schema(
    tsv_file: Path,
    db_name: str,
    table_name: str,
    output_sql_file: Path,
    timestamp_col: str = "@timestamp",
    orderby_cols: str = "meta.device,meta.name,@timestamp",
    required_string_cols: str = "meta.device,meta.name",
):
    """
    Reads the header of a TSV file and generates a ClickHouse CREATE TABLE SQL schema.

    Args:
        tsv_file: Path to the input TSV file.
        db_name: Name of the ClickHouse database.
        table_name: Name of the ClickHouse table.
        output_sql_file: Path to save the generated SQL file.
        timestamp_col: Name of the timestamp column in the TSV header.
        orderby_cols: Comma-separated string of columns for ORDER BY clause.
        required_string_cols: Comma-separated string of columns for non-nullable String.
    """
    print(f"Reading header from TSV file: {tsv_file}")

    try:
        with open(tsv_file, "r", newline="", encoding="utf-8") as tsvfile:
            reader = csv.reader(tsvfile, delimiter="\t")
            try:
                header = [h.strip() for h in next(reader)]
            except StopIteration:
                print(
                    f"Error: TSV file '{tsv_file}' appears to be empty.",
                    file=sys.stderr,
                )
                sys.exit(1)
            except Exception as e:
                print(f"Error reading header from '{tsv_file}': {e}", file=sys.stderr)
                sys.exit(1)

        if not header:
            print(
                f"Error: Could not read a valid header from '{tsv_file}'.",
                file=sys.stderr,
            )
            sys.exit(1)

        print(f"Detected {len(header)} columns in header.")

        # --- Process Columns ---
        column_definitions = []
        processed_cols = set()

        # Helper sets for quick lookup
        orderby_cols_set = set(c.strip() for c in orderby_cols.split(",") if c.strip())
        required_string_cols_set = set(
            c.strip() for c in required_string_cols.split(",") if c.strip()
        )

        # Validate timestamp column exists
        if timestamp_col not in header:
            print(
                f"Error: Specified timestamp column '{timestamp_col}' not found in TSV header.",
                file=sys.stderr,
            )
            print(f"Available headers: {header}", file=sys.stderr)
            sys.exit(1)

        # Validate required string columns exist
        missing_req_strings = required_string_cols_set - set(header)
        if missing_req_strings:
            print(
                f"Error: Specified required string columns not found in TSV header: {missing_req_strings}",
                file=sys.stderr,
            )
            sys.exit(1)

        for col_name in header:
            if not col_name:
                print(
                    "Warning: Found empty column name in header, skipping.",
                    file=sys.stderr,
                )
                continue

            quoted_col_name = f"`{col_name}`"
            col_type = "Nullable(String)"

            if col_name == timestamp_col:
                col_type = "DateTime64(3, 'UTC')"
                processed_cols.add(col_name)
            elif col_name in required_string_cols_set:
                col_type = "String"
                processed_cols.add(col_name)
            # Check for numeric prefix *after* specific required types
            elif col_name.startswith("values."):
                col_type = "Nullable(Float64)"
                processed_cols.add(col_name)

            column_definitions.append(f"    {quoted_col_name} {col_type}")

        column_definitions_str = ",\n".join(column_definitions)

        # --- Format ORDER BY Clause ---
        orderby_clause_parts = []
        missing_orderby_cols = []
        for col in orderby_cols_set:
            if col in header:
                orderby_clause_parts.append(f"`{col}`")
            else:
                missing_orderby_cols.append(col)

        if missing_orderby_cols:
            print(
                f"Error: Columns specified for ORDER BY ({orderby_cols}) not found in TSV header: {missing_orderby_cols}",
                file=sys.stderr,
            )
            sys.exit(1)

        if not orderby_clause_parts:
            print(
                "Error: ORDER BY clause cannot be empty. Please specify valid --orderby_cols.",
                file=sys.stderr,
            )
            sys.exit(1)

        orderby_clause_str = f"({', '.join(orderby_clause_parts)})"

        # --- Format PARTITION BY Clause ---
        partition_by_clause_str = f"toYYYYMM(`{timestamp_col}`)"

        # --- Construct CREATE TABLE Statement ---
        create_table_sql = f"""-- Generated Schema from {tsv_file.name}
-- Target Database: {db_name}
-- Target Table: {table_name}

CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` (
{column_definitions_str}
)
ENGINE = MergeTree()
PARTITION BY {partition_by_clause_str}
ORDER BY {orderby_clause_str};
"""

        # --- Output ---
        print("\n--- Generated ClickHouse Schema ---")
        print(create_table_sql)
        print("-----------------------------------")

        try:
            with open(output_sql_file, "w", encoding="utf-8") as f_out:
                f_out.write(create_table_sql)
            print(f"Schema successfully written to: {output_sql_file}")
        except IOError as e:
            print(
                f"Error writing schema to file '{output_sql_file}': {e}",
                file=sys.stderr,
            )
            sys.exit(1)
        except Exception as e:
            print(
                f"An unexpected error occurred writing the schema file: {e}",
                file=sys.stderr,
            )
            sys.exit(1)

    except FileNotFoundError:
        print(f"Error: Input TSV file not found at '{tsv_file}'", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a ClickHouse CREATE TABLE schema from a TSV header.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--tsv_file",
        required=True,
        type=Path,
        help="Path to the input TSV file (only header is read).",
    )
    parser.add_argument(
        "--db_name", required=True, help="Name of the ClickHouse database."
    )
    parser.add_argument(
        "--table_name", required=True, help="Name of the ClickHouse table."
    )
    parser.add_argument(
        "--output_sql_file",
        required=True,
        type=Path,
        help="Path to save the generated CREATE TABLE SQL file.",
    )
    parser.add_argument(
        "--timestamp_col",
        default="@timestamp",
        help="Name of the timestamp column in the TSV header (for DateTime64 type and PARTITION BY).",
    )
    parser.add_argument(
        "--orderby_cols",
        default="meta.device,meta.name,@timestamp",
        help="Comma-separated list of column names for the ORDER BY clause.",
    )
    parser.add_argument(
        "--required_string_cols",
        default="meta.device,meta.name",
        help="Comma-separated list of column names to define as non-nullable String.",
    )

    args = parser.parse_args()

    # Call the function using attribute access on args object
    generate_schema(
        tsv_file=args.tsv_file,
        db_name=args.db_name,
        table_name=args.table_name,
        output_sql_file=args.output_sql_file,
        timestamp_col=args.timestamp_col,
        orderby_cols=args.orderby_cols,
        required_string_cols=args.required_string_cols,
    )
