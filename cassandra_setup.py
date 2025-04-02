import csv
import os
import random

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import argparse
from tqdm import tqdm
import sys

from datetime_script import generate_timestamp


def create_keyspace_and_tables(session, keyspace_name="instacart", replication_factor=1):
    """Create the keyspace and tables for the Instacart dataset."""
    print("Creating keyspace and tables...")

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {replication_factor} }}
    """)

    session.execute(f"USE {keyspace_name}")

    session.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id int PRIMARY KEY,
            product_name text,
            aisle_id int,
            department_id int
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS aisles (
            aisle_id int PRIMARY KEY,
            aisle text
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            department_id int PRIMARY KEY,
            department text
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id int PRIMARY KEY,
            user_id int,
            order_number int,
            order_dow int,
            order_timestamp timestamp,
            days_since_prior_order int
        )
    """)

    # Modified order_products table to include order_timestamp and product_name
    session.execute("""
        CREATE TABLE IF NOT EXISTS order_products ( 
            order_id int,
            product_id int,
            add_to_cart_order int,
            reordered int,
            order_timestamp timestamp,
            product_name text,
            PRIMARY KEY (order_id, product_id)
        )
    """)

    print("Keyspace and tables created successfully")


def load_products(session, data_dir):
    """Load products data from CSV."""
    print("Loading products data...")
    products_file = os.path.join(data_dir, "products.csv")

    if not os.path.exists(products_file):
        print(f"Error: {products_file} not found!")
        return

    insert_query = """
        INSERT INTO products (product_id, product_name, aisle_id, department_id)
        VALUES (?, ?, ?, ?)
    """
    prepared_stmt = session.prepare(insert_query)

    with open(products_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)

        batch_size = 100
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_count = 0
        total_count = 0

        for row in tqdm(reader, desc="Products"):
            product_id = int(row[0])
            product_name = row[1]
            aisle_id = int(row[2])
            department_id = int(row[3])

            batch.add(prepared_stmt, (product_id, product_name, aisle_id, department_id))
            batch_count += 1

            if batch_count >= batch_size:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                batch_count = 0
                total_count += batch_size

        if batch_count > 0:
            session.execute(batch)
            total_count += batch_count

    print(f"Loaded {total_count} products")


def load_aisles(session, data_dir):
    """Load aisles data from CSV."""
    print("Loading aisles data...")
    aisles_file = os.path.join(data_dir, "aisles.csv")

    if not os.path.exists(aisles_file):
        print(f"Error: {aisles_file} not found!")
        return

    insert_query = """
        INSERT INTO aisles (aisle_id, aisle)
        VALUES (?, ?)
    """
    prepared_stmt = session.prepare(insert_query)

    with open(aisles_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)

        for row in tqdm(reader, desc="Aisles"):
            aisle_id = int(row[0])
            aisle = row[1]

            session.execute(prepared_stmt, (aisle_id, aisle))

    print("Aisles data loaded successfully")


def load_departments(session, data_dir):
    """Load departments data from CSV."""
    print("Loading departments data...")
    departments_file = os.path.join(data_dir, "departments.csv")

    if not os.path.exists(departments_file):
        print(f"Error: {departments_file} not found!")
        return

    insert_query = """
        INSERT INTO departments (department_id, department)
        VALUES (?, ?)
    """
    prepared_stmt = session.prepare(insert_query)

    with open(departments_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)

        for row in tqdm(reader, desc="Departments"):
            department_id = int(row[0])
            department = row[1]

            session.execute(prepared_stmt, (department_id, department))

    print("Departments data loaded successfully")


def load_orders(session, data_dir):
    """Load orders data from CSV."""
    print("Loading orders data...")
    orders_file = os.path.join(data_dir, "orders.csv")

    if not os.path.exists(orders_file):
        print(f"Error: {orders_file} not found!")
        return

    insert_query = """
        INSERT INTO orders (order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    prepared_stmt = session.prepare(insert_query)

    order_timestamps = {}

    with open(orders_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)

        batch_size = 100
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_count = 0
        total_count = 0

        for row in tqdm(reader, desc="Orders"):
            order_id = int(row[0])
            user_id = int(row[1])
            order_number = int(row[3])
            order_dow = int(row[4])
            order_timestamp = generate_timestamp(hour=int(row[5]), days_offset=int(row[6]))

            order_timestamps[order_id] = order_timestamp

            days_since_prior_order = None
            if row[6] and row[6] != "":
                days_since_prior_order = int(float(row[6]))

            batch.add(prepared_stmt,
                      (order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order))
            batch_count += 1

            if batch_count >= batch_size:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                batch_count = 0
                total_count += batch_size

        if batch_count > 0:
            session.execute(batch)
            total_count += batch_count

    print(f"Loaded {total_count} orders")
    return order_timestamps


def load_order_products(session, data_dir, order_timestamps):
    """Load order products data from CSV."""
    print("Loading order products data...")
    order_products_file = os.path.join(data_dir, "orders_products.csv")

    files_to_process = []
    if os.path.exists(order_products_file):
        files_to_process.append(order_products_file)
    else:
        print(f"Warning: {order_products_file} not found")

    if not files_to_process:
        print("Error: No order products files found!")
        return

    product_names = {}
    print("Loading product names into memory...")
    query = "SELECT product_id, product_name FROM products"
    rows = session.execute(query)
    for row in rows:
        product_names[row.product_id] = row.product_name
    print(f"Loaded {len(product_names)} product names")

    insert_query = """
        INSERT INTO order_products (order_id, product_id, add_to_cart_order, reordered, order_timestamp, product_name)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    prepared_stmt = session.prepare(insert_query)

    total_count = 0

    for file_path in files_to_process:
        file_name = os.path.basename(file_path)
        print(f"Processing {file_name}...")

        line_count = sum(1 for _ in open(file_path, 'r', encoding='utf-8')) - 1

        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)

            batch_size = 100
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_count = 0
            file_count = 0

            for row in tqdm(reader, total=line_count, desc=file_name):
                order_id = int(row[0])
                product_id = int(row[1])
                add_to_cart_order = int(row[2])
                reordered = int(row[3])

                # Get the order timestamp from our dictionary
                order_timestamp = order_timestamps.get(order_id)

                # Get the product name from our dictionary
                product_name = product_names.get(product_id, "Unknown Product")

                batch.add(prepared_stmt,
                          (order_id, product_id, add_to_cart_order, reordered, order_timestamp, product_name))
                batch_count += 1

                if batch_count >= batch_size:
                    session.execute(batch)
                    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                    batch_count = 0
                    file_count += batch_size

            if batch_count > 0:
                session.execute(batch)
                file_count += batch_count

            total_count += file_count
            print(f"Loaded {file_count} records from {file_name}")

    print(f"Loaded {total_count} order products records in total")


def create_indexes(session):
    """Create indexes for better query performance."""
    print("Creating indexes...")

    session.execute("CREATE INDEX IF NOT EXISTS ON orders (user_id)")

    session.execute("CREATE INDEX IF NOT EXISTS ON products (aisle_id)")
    session.execute("CREATE INDEX IF NOT EXISTS ON products (department_id)")

    # Add index for the new fields in order_products
    session.execute("CREATE INDEX IF NOT EXISTS ON order_products (order_timestamp)")
    session.execute("CREATE INDEX IF NOT EXISTS ON order_products (product_name)")

    print("Indexes created successfully")


def main():
    parser = argparse.ArgumentParser(description="Set up Cassandra keyspace and tables for Instacart dataset.")
    parser.add_argument("--host", default="localhost", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra port")
    parser.add_argument("--keyspace", default="instacart", help="Keyspace name")
    parser.add_argument("--replication-factor", type=int, default=1, help="Replication factor")
    parser.add_argument("--data-dir", required=True, help="Directory containing the Instacart CSV files")
    parser.add_argument("--skip-tables", action="store_true", help="Skip table creation")
    parser.add_argument("--skip-indexes", action="store_true", help="Skip index creation")
    parser.add_argument("--skip-aisles", action="store_true", help="Skip loading aisles data")
    parser.add_argument("--skip-departments", action="store_true", help="Skip loading departments data")
    parser.add_argument("--skip-products", action="store_true", help="Skip loading products data")
    parser.add_argument("--skip-orders", action="store_true", help="Skip loading orders data")
    parser.add_argument("--skip-order-products", action="store_true", help="Skip loading order products data")

    args = parser.parse_args()

    if not os.path.exists(args.data_dir):
        print(f"Error: Data directory {args.data_dir} does not exist.")
        sys.exit(1)

    print(f"Connecting to Cassandra at {args.host}:{args.port}...")
    cluster = Cluster([args.host], port=args.port)
    session = cluster.connect()

    try:
        if not args.skip_tables:
            create_keyspace_and_tables(session, args.keyspace, args.replication_factor)

        session.execute(f"USE {args.keyspace}")

        if not args.skip_aisles:
            load_aisles(session, args.data_dir)

        if not args.skip_departments:
            load_departments(session, args.data_dir)

        if not args.skip_products:
            load_products(session, args.data_dir)

        order_timestamps = {}
        if not args.skip_orders:
            order_timestamps = load_orders(session, args.data_dir)

        if not args.skip_order_products:
            load_order_products(session, args.data_dir, order_timestamps)

        if not args.skip_indexes:
            create_indexes(session)

        print("Data loading completed successfully.")

    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()