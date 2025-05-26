import psycopg2
import pymongo
import cassandra.cluster
import mariadb
import argparse
import csv
import os
import timeit
import matplotlib.pyplot as plt

from DataProvider import DataProvider
from Database import Database

def log_execution_time(db_type, queries, execution_time):
    """Log execution time to a CSV file."""
    return
    with open("execution_times.csv", "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([db_type, "; ".join(queries), execution_time])

def save_test_result(db_type, test_name, number_of_queries, execution_time):
    folder_path = f"./results/{test_name}/"
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{db_type}.csv")

    with open(file_path, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([number_of_queries, execution_time])

    all_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

    plt.figure(figsize=(10, 6))
    for filename in all_files:
        full_path = os.path.join(folder_path, filename)
        db_label = filename.replace(".csv", "")

        x_vals = []
        y_vals = []

        with open(full_path, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                try:
                    x_vals.append(int(row[0]))
                    y_vals.append(float(row[1]))
                except ValueError:
                    continue

        sorted_pairs = sorted(zip(x_vals, y_vals), key=lambda pair: pair[0])
        x_vals, y_vals = zip(*sorted_pairs)

        plt.plot(x_vals, y_vals, marker='o', label=db_label)

    plt.xlabel("Number of Records")
    plt.ylabel("Execution Time (s)")
    plt.title(f"Execution Time Comparison for Test: {test_name}")
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(os.path.join(folder_path, f"{test_name}.png"))
    plt.close()


def connect_to_postgresql(db_name, user, password, host="localhost", port=5432):
    return psycopg2.connect(database=db_name, user=user, password=password, host=host, port=port)


def connect_to_mongodb(host="localhost", port=27017):
    return pymongo.MongoClient(host, port)


def connect_to_cassandra(contact_points=["localhost"], port=9042):
    cluster = cassandra.cluster.Cluster(contact_points, port=port)
    return cluster.connect()


def connect_to_mariadb(db_name, user, password, host="localhost", port=3306):
    return mariadb.connect(user=user, password=password, host=host, port=port, database=db_name)


def execute_sql_queries(connection, query, db_type, records_number, number_of_query_executions=1):
    cursor = connection.cursor()

    def execute_query():
        try:
            if db_type == "PostgreSQL":
                cursor.execute("BEGIN")
            elif db_type == "MariaDB":
                cursor.execute("START TRANSACTION")
            cursor.execute(query)
        except Exception as e:
            print(f"Error during execution: {e}")
        finally:
            connection.rollback()

    execution_time = timeit.timeit(execute_query, number=number_of_query_executions)
    avg_execution_time = execution_time / number_of_query_executions
    print(f"{db_type} average execution time per {number_of_query_executions} calls: {avg_execution_time} seconds for {records_number} records")
    log_execution_time(db_type, query, execution_time)

    return avg_execution_time

def execute_cassandra_queries(session, query, db_type, records_number, number_of_query_executions=1):
    def execute_query():
        try:
            session.execute(query)
        except Exception as e:
            print(f"Error during execution: {e}")

    execution_time = timeit.timeit(execute_query, number=number_of_query_executions)
    avg_execution_time = execution_time / number_of_query_executions
    print(f"{db_type} average execution time per {number_of_query_executions} calls: {avg_execution_time} seconds for {records_number} records")
    log_execution_time(db_type, [query], execution_time)

    return avg_execution_time


def execute_mongo_queries(client, db_name, query, number_of_query_executions=1):
    db = client[db_name]
    collection_name, operation, params, limit = query
    collection = db[collection_name]

    def execute_queries():
        if operation == "find" and limit is not None:
            collection.find(*params).limit(limit)
            # print(list(res))
        else:
            getattr(collection, operation)(*params)
            # print(list(res))

    execution_time = timeit.timeit(execute_queries, number=number_of_query_executions)
    avg_execution_time = execution_time / number_of_query_executions
    print(f"MongoDB average execution time per {number_of_query_executions} calls: {avg_execution_time} seconds")
    log_execution_time("MongoDB", query, execution_time)

    return avg_execution_time


def load_database_credentials() -> dict:
    """Load credentials from environment variables or a config."""
    return {
        "postgres": {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "db_name": os.getenv("POSTGRES_DB", "instacart"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "user": os.getenv("POSTGRES_USER", "root"),
            "password": os.getenv("POSTGRES_PASSWORD", "root")
        },
        "mongo": {
            "host": os.getenv("MONGO_HOST", "localhost"),
            "port": int(os.getenv("MONGO_PORT", 27017))
        },
        "cassandra": {
            "contact_points": os.getenv("CASSANDRA_CONTACT_POINTS", "localhost").split(","),
            "port": int(os.getenv("CASSANDRA_PORT", 9042))
        },
        "mariadb": {
            "host": os.getenv("MARIADB_HOST", "localhost"),
            "db_name": os.getenv("MARIADB_DB", "instacart"),
            "port": int(os.getenv("MARIADB_PORT", 3306)),
            "user": os.getenv("MARIADB_USER", "user"),
            "password": os.getenv("MARIADB_PASSWORD", "user_password")
        },
    }


def main(db_type, records_number, test_name, number_of_query_executions):
    credentials = load_database_credentials()

    if db_type == "postgres":
        run_postgres(credentials, records_number, test_name, number_of_query_executions)
    elif db_type == "mongo":
        run_mongo(credentials, records_number, test_name, number_of_query_executions)
    elif db_type == "cassandra":
        run_cassandra(credentials, records_number, test_name, number_of_query_executions)
    elif db_type == "mariadb":
        run_mariadb(credentials, records_number, test_name, number_of_query_executions)


def run_mariadb(credentials, records_number, test_name, number_of_query_executions, return_time=False):
    mariadb = Database(
        credentials["mariadb"]["host"],
        credentials["mariadb"]["db_name"],
        credentials["mariadb"]["port"],
        credentials["mariadb"]["user"],
        credentials["mariadb"]["password"]
    )
    connection = connect_to_mariadb(
        mariadb.db_name,
        mariadb.user,
        mariadb.password,
        mariadb.host,
        mariadb.port
    )
    queries = DataProvider.get_mariadb_queries(test_name, records_number)
    execution_time = execute_sql_queries(connection, queries, "MariaDB", records_number, number_of_query_executions)
    save_test_result('mariadb', test_name, records_number, execution_time)
    connection.close()
    if return_time:
        return execution_time


def run_cassandra(credentials, records_number, test_name, number_of_query_executions, return_time=False):
    cassandra = Database(
        credentials["cassandra"]["contact_points"][0],
        None,
        credentials["cassandra"]["port"]
    )
    client = connect_to_cassandra([cassandra.host], cassandra.port)
    query = DataProvider.get_cassandra_queries(test_name, records_number)
    execution_time = execute_cassandra_queries(client, query, "Cassandra", records_number, number_of_query_executions)
    save_test_result('cassandra', test_name, records_number, execution_time)
    if return_time:
        return execution_time


def run_mongo(credentials, number_of_queries, test_name, number_of_query_executions, return_time=False):
    mongo = Database(
        credentials["mongo"]["host"],
        None,
        credentials["mongo"]["port"]
    )
    client = connect_to_mongodb(mongo.host, mongo.port)
    queries = DataProvider.get_mongo_queries(test_name, number_of_queries)
    execution_time = execute_mongo_queries(client, "instacart", queries, number_of_query_executions)
    save_test_result('mongo', test_name, number_of_queries, execution_time)
    if return_time:
        return execution_time


def run_postgres(credentials, records_number, test_name, number_of_query_executions, return_time=False):
    postgres = Database(
        credentials["postgres"]["host"],
        credentials["postgres"]["db_name"],
        credentials["postgres"]["port"],
        credentials["postgres"]["user"],
        credentials["postgres"]["password"]
    )
    connection = connect_to_postgresql(
        postgres.db_name,
        postgres.user,
        postgres.password,
        postgres.host,
        postgres.port
    )
    queries = DataProvider.get_postgres_queries(test_name, records_number)
    execution_time = execute_sql_queries(connection, queries, "PostgreSQL", records_number, number_of_query_executions)
    save_test_result('postgres', test_name, records_number, execution_time)
    connection.close()
    if return_time:
        return execution_time

test_names = ["insert_base", 
              "insert_multi", 
              "select_base", 
              "select_join", 
              "select_date", 
              "update_base", 
              "delete_base", 
            # To remove
              "insert",
              "update", 
              "delete", 
              "select"
            ]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database query execution script.")
    parser.add_argument("--db_type", type=str, required=True, choices=["postgres", "mongo", "cassandra", "mariadb"], help="Type of the database.")
    parser.add_argument("--records_num", type=int, default=1, help="Number of records to retrieve.")
    parser.add_argument("--test_name", type=str, required=True, choices=test_names, help="Type of the queries to execute.")
    parser.add_argument("--executions_num", type=int, default=1, help="Number of times to execute the entire set of queries.")
    args = parser.parse_args()

    main(args.db_type, args.records_num, args.test_name, args.executions_num)
