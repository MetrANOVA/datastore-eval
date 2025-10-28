# runners/clickhouse_runner.py
import sys
import time

from clickhouse_driver import Client

# --- Database Connection ---
CLICKHOUSE_HOST = "your-clickhouse-vm-ip"
client = Client(CLICKHOUSE_HOST)


def get_queries():
    """Maps test names to ClickHouse SQL queries."""
    return {
        "daily_average": "SELECT toStartOfDay(timestamp) AS day, avg(value) FROM your_table GROUP BY day",
        "last_point": "SELECT argMax(value, timestamp), argMax(timestamp, timestamp) FROM your_table GROUP BY sensor_id",
        # Add more test queries here
    }


if __name__ == "__main__":
    test_name = sys.argv[1]  # Get test name from command-line argument
    queries = get_queries()

    if test_name not in queries:
        print("Error: Test name not found.", file=sys.stderr)
        sys.exit(1)

    sql_query = queries[test_name]

    start_time = time.perf_counter()
    # Execute the query
    client.execute(sql_query)
    end_time = time.perf_counter()

    # CRITICAL: Print the execution time to stdout
    print(end_time - start_time)
