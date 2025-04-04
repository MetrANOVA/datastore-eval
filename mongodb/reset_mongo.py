from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "myTimeSeriesDB"

try:
    client = MongoClient(MONGO_URI)

    print(f"Attempting to drop database '{DB_NAME}'...")
    client.drop_database(DB_NAME)
    print(f"Database '{DB_NAME}' dropped successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if "client" in locals() and client:
        client.close()
        print("Connection closed.")
