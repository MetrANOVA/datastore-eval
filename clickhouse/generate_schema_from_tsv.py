# generate_ch_schema_from_tsv.py
import argparse
import csv
import sys
from pathlib import Path

# --- Configuration for TSV -> ClickHouse mapping ---
TSV_TIMESTAMP_COL = "@timestamp"
TSV_NODE_COL = "meta.device"
TSV_INTERFACE_COL = "meta.name"
DISCARD_TSV_FIELDS = {"@collect_time_min", "@exit_time", "@processing_time"}


def generate_schema(
    input_tsv: Path, db_name: str, table_name: str, output_sql_file: Path
):
    """
    Reads the header of a TSV file and generates a ClickHouse CREATE TABLE SQL schema,
    transforming field names as needed for the database.
    """
    print(f"Reading header from TSV file: {input_tsv}")
    try:
        with open(input_tsv, "r", newline="", encoding="utf-8") as tsvfile:
            reader = csv.reader(tsvfile, delimiter="\t")
            header = [h.strip() for h in next(reader)]
    except StopIteration:
        print(f"Error: TSV file '{input_tsv}' appears to be empty.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading header from '{input_tsv}': {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Detected {len(header)} columns in header.")

    column_definitions = []
    # These will be the final column names as they appear in the ClickHouse table
    final_ch_orderby_cols = []
    final_ch_ts_col = ""

    # Process columns to generate schema definitions
    for tsv_col_name in header:
        if tsv_col_name in DISCARD_TSV_FIELDS:
            continue

        target_ch_col_name = tsv_col_name
        col_type = "Nullable(String)"  # Default type

        if tsv_col_name == TSV_TIMESTAMP_COL:
            target_ch_col_name = "timestamp"  # Use a cleaner name in the database
            col_type = "DateTime64(3, 'UTC')"
            final_ch_ts_col = f"`{target_ch_col_name}`"
        elif tsv_col_name == TSV_NODE_COL:
            target_ch_col_name = "device"
            col_type = "String"  # Make required identifiers non-nullable
            final_ch_orderby_cols.append(f"`{target_ch_col_name}`")
        elif tsv_col_name == TSV_INTERFACE_COL:
            target_ch_col_name = "interfaceName"
            col_type = "String"  # Make required identifiers non-nullable
            final_ch_orderby_cols.append(f"`{target_ch_col_name}`")
        elif tsv_col_name.startswith("values."):
            target_ch_col_name = tsv_col_name.replace("values.", "", 1)
            col_type = "Nullable(Float64)"
        elif tsv_col_name.startswith("meta."):
            target_ch_col_name = tsv_col_name.replace("meta.", "", 1)
            col_type = "Nullable(String)"
        # Else, it's a general field, use original name and default type Nullable(String)

        # Quote the final column name for safety in SQL
        quoted_col_name = f"`{target_ch_col_name}`"
        column_definitions.append(f"    {quoted_col_name} {col_type}")

    if not final_ch_ts_col:
        print(
            f"Error: Timestamp column '{TSV_TIMESTAMP_COL}' not found in TSV header.",
            file=sys.stderr,
        )
        sys.exit(1)
    if len(final_ch_orderby_cols) < 2:
        print(
            f"Error: One or both key identifier columns ('{TSV_NODE_COL}', '{TSV_INTERFACE_COL}') not found.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Add timestamp to the ORDER BY clause
    final_ch_orderby_cols.append(final_ch_ts_col)

    column_definitions_str = ",\n".join(column_definitions)
    orderby_clause_str = f"({', '.join(final_ch_orderby_cols)})"
    partition_by_clause_str = f"toYYYYMM({final_ch_ts_col})"

    create_table_sql = f"""-- Generated Schema from {input_tsv.name}
-- Target Database: {db_name}
-- Target Table: {table_name}

CREATE TABLE IF NOT EXISTS `{db_name}`.`{table_name}` (
{column_definitions_str}
)
ENGINE = MergeTree()
PARTITION BY {partition_by_clause_str}
ORDER BY {orderby_clause_str};
"""
    print("\n--- Generated ClickHouse Schema ---")
    print(create_table_sql)
    print("-----------------------------------")

    try:
        output_sql_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_sql_file, "w", encoding="utf-8") as f_out:
            f_out.write(create_table_sql)
        print(f"Schema successfully written to: {output_sql_file}")
    except Exception as e:
        print(f"Error writing schema to file '{output_sql_file}': {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a ClickHouse CREATE TABLE schema from a TSV header.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input_tsv",
        type=Path,
        required=True,
        help="Path to the original large TSV file to inspect its header.",
    )
    parser.add_argument(
        "--db_name",
        required=True,
        help="Name of the ClickHouse database for the CREATE TABLE statement.",
    )
    parser.add_argument(
        "--table_name",
        required=True,
        help="Name of the ClickHouse table for the CREATE TABLE statement.",
    )
    parser.add_argument(
        "--output_sql_file",
        type=Path,
        required=True,
        help="Path to save the generated CREATE TABLE SQL file.",
    )
    args = parser.parse_args()
    generate_schema(args.input_tsv, args.db_name, args.table_name, args.output_sql_file)
