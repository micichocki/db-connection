import csv
import os
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import argparse
from tqdm import tqdm
import sys

from datetime_script import generate_timestamp


def create_keyspace_and_tables(session, keyspace_name="instacart", replication_factor=1):
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

    session.execute("""
        CREATE TABLE IF NOT EXISTS orders_by_timestamp (
            order_timestamp timestamp,
            order_id int,
            user_id int,
            order_number int,
            PRIMARY KEY (order_timestamp, order_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS order_products_by_order ( 
            order_id int,
            product_id int,
            user_id int,
            order_number int,
            order_dow int,
            order_timestamp timestamp,
            days_since_prior_order int,
            product_name text,
            add_to_cart_order int,
            reordered int,
            PRIMARY KEY (order_id, product_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id int PRIMARY KEY,
            name text
        )
    """)

    print("Keyspace and tables created successfully")


def load_products(session, data_dir):
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

    insert_by_timestamp_query = """
        INSERT INTO orders_by_timestamp (order_timestamp, order_id, user_id, order_number)
        VALUES (?, ?, ?, ?)
    """
    prepared_by_timestamp_stmt = session.prepare(insert_by_timestamp_query)

    order_data = {}

    with open(orders_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)

        batch_size = 100
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_timestamp = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_count = 0
        total_count = 0

        for row in tqdm(reader, desc="Orders"):
            order_id = int(row[0])
            user_id = int(row[1])
            order_number = int(row[3])
            order_dow = int(row[4])
            order_timestamp = datetime.strptime(
                generate_timestamp(hour=int(float(row[5])), days_offset=int(float(row[6] or 0))), "%Y-%m-%d %H:%M:%S")
            days_since_prior_order = int(float(row[6])) if row[6] and row[6] != "" else 0
            order_data[order_id] = {
                'user_id': user_id,
                'order_number': order_number,
                'order_dow': order_dow,
                'order_timestamp': order_timestamp,
                'days_since_prior_order': days_since_prior_order
            }

            batch.add(prepared_stmt,
                      (order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order))
            batch_timestamp.add(prepared_by_timestamp_stmt,
                                (order_timestamp, order_id, user_id, order_number))
            batch_count += 1

            if batch_count >= batch_size:
                session.execute(batch)
                session.execute(batch_timestamp)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                batch_timestamp = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                batch_count = 0
                total_count += batch_size

        if batch_count > 0:
            session.execute(batch)
            session.execute(batch_timestamp)
            total_count += batch_count

    print(f"Loaded {total_count} orders")
    return order_data


def load_order_products_by_order(session, data_dir, order_data):
    print("Loading order products data into order_products_by_order...")
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
        INSERT INTO order_products_by_order (
            order_id, product_id, user_id, order_number, order_dow,
            order_timestamp, days_since_prior_order, product_name,
            add_to_cart_order, reordered
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

                order_info = order_data.get(order_id, {})
                if not order_info:
                    print(f"Warning: No order data found for order_id {order_id}")
                    continue

                product_name = product_names.get(product_id, "Unknown Product")

                batch.add(prepared_stmt, (
                    order_id,
                    product_id,
                    order_info.get('user_id'),
                    order_info.get('order_number'),
                    order_info.get('order_dow'),
                    order_info.get('order_timestamp'),
                    order_info.get('days_since_prior_order'),
                    product_name,
                    add_to_cart_order,
                    reordered
                ))
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


def load_users(session):
    print("Loading users data...")

    insert_query = """
        INSERT INTO users (user_id, name)
        VALUES (?, ?)
    """
    prepared_stmt = session.prepare(insert_query)

    batch_size = 100
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch_count = 0
    total_count = 0

    for user_id in tqdm(range(1, 206210), desc="Users"):
        name = f"User{user_id}"

        batch.add(prepared_stmt, (user_id, name))
        batch_count += 1

        if batch_count >= batch_size:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_count = 0
            total_count += batch_size

    if batch_count > 0:
        session.execute(batch)
        total_count += batch_count

    print(f"Loaded {total_count} users")


def create_indexes(session):
    print("Creating indexes...")

    session.execute("CREATE INDEX IF NOT EXISTS ON orders (user_id)")

    session.execute("CREATE INDEX IF NOT EXISTS ON products (aisle_id)")
    session.execute("CREATE INDEX IF NOT EXISTS ON products (department_id)")

    session.execute("CREATE INDEX IF NOT EXISTS ON order_products_by_order (user_id)")
    session.execute("CREATE INDEX IF NOT EXISTS ON order_products_by_order (product_name)")
    session.execute("CREATE INDEX IF NOT EXISTS ON order_products_by_order (order_timestamp)")

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
    parser.add_argument("--skip-users", action="store_true", help="Skip loading users data")

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

        order_data = {}
        if not args.skip_orders:
            order_data = load_orders(session, args.data_dir)

        if not args.skip_order_products:
            load_order_products_by_order(session, args.data_dir, order_data)

        if not args.skip_users:
            load_users(session)

        if not args.skip_indexes:
            create_indexes(session)

        print("Data loading completed successfully.")

    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()