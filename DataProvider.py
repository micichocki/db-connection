from datetime_script import generate_timestamp
from datetime import datetime

class DataProvider:
    @staticmethod
    def get_postgres_queries(test_name, records_number):
        if test_name == "select_join":
            return f"""
                SELECT o.order_id, o.user_id, o.order_number, 
                       p.product_id, p.product_name, p.department_id,
                       op.add_to_cart_order, op.reordered
                FROM orders o
                JOIN orders_products op ON o.order_id = op.order_id
                JOIN products p ON op.product_id = p.product_id
                LIMIT {records_number}
            """
        elif test_name == "select_base":
            return f"""
                SELECT order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order
                FROM orders
                LIMIT {records_number}
            """
        elif test_name == "select_date":
            ts = generate_timestamp(1, 1)
            return f"""
                SELECT order_id, user_id, order_number, order_timestamp
                FROM orders
                WHERE order_timestamp >= '{ts}'
                LIMIT {records_number}
            """
        elif test_name == "insert_base":
            values_list = []
            for i in range(1, records_number + 1):
                user_id = 300000 + i
                values_list.append(f"('User{user_id}')")

            batch_query = f"""
                INSERT INTO users (name)
                VALUES {', '.join(values_list)};
            """
            return batch_query
        elif test_name == "insert_multi":
            orders_values = []
            orders_products_values = []

            for i in range(1, records_number + 1):
                user_id = 300000 + i
                order_id = 5000000 + i
                order_number = i
                order_dow = i % 7
                ts = generate_timestamp(i % 24, i % 30)
                days_since_prior = i % 30

                orders_values.append(
                    f"({order_id}, {user_id}, {order_number}, {order_dow}, '{ts}', {days_since_prior})"
                )

                for j in range(1, min(3, records_number + 1)):
                    product_id = 50000 + i
                    orders_products_values.append(
                        f"({order_id}, {product_id}, {j}, {i % 2})"
                    )

            batch_query = f"""
                INSERT INTO orders (order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order)
                VALUES {', '.join(orders_values)};

                INSERT INTO orders_products (order_id, product_id, add_to_cart_order, reordered)
                VALUES {', '.join(orders_products_values)};
            """
            return batch_query
        elif test_name == "update_base":
            return f"""
                UPDATE aisles
                SET aisle = 'Updated Aisle'
                WHERE aisle_id BETWEEN 1 AND {records_number};
            """
        elif test_name == "delete_base":
            return f"""
                DELETE FROM orders 
                WHERE order_id BETWEEN 1 AND {records_number};
            """
        elif test_name == "delete_multi":
            return f"""
                DELETE FROM orders_products 
                WHERE order_id BETWEEN 1 AND {records_number};

                DELETE FROM orders 
                WHERE order_id BETWEEN 1 AND {records_number};
            """

        return ""

    @staticmethod
    def get_mariadb_queries(test_name, records_number):
        if test_name == "select" or test_name == "select_join":
            return f"""
                SELECT o.order_id, o.user_id, o.order_number, 
                       p.product_id, p.product_name, p.department_id,
                       op.add_to_cart_order, op.reordered
                FROM orders o
                JOIN orders_products op ON o.order_id = op.order_id
                JOIN products p ON op.product_id = p.product_id
                LIMIT {records_number}
            """
        elif test_name == "select_base":
            return f"""
                SELECT order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order
                FROM orders
                LIMIT {records_number}
            """
        elif test_name == "select_date":
            ts = generate_timestamp(1, 1)
            return f"""
                SELECT order_id, user_id, order_number, order_timestamp
                FROM orders
                WHERE order_timestamp >= '{ts}'
                LIMIT {records_number}
            """
        elif test_name == "insert_base":
            values_list = []
            for i in range(1, records_number + 1):
                user_id = 300000 + i
                values_list.append(f"({user_id}, 'User{user_id}')")

            batch_query = f"""
                INSERT INTO users (user_id, name)
                VALUES {', '.join(values_list)};
            """
            return batch_query
        elif test_name == "insert_multi":
            orders_values = []
            orders_products_values = []

            for i in range(1, records_number + 1):
                user_id = 300000 + i
                order_id = 5000000 + i
                order_number = i
                order_dow = i % 7
                ts = generate_timestamp(i % 24, i % 30)
                days_since_prior = i % 30

                orders_values.append(
                    f"({order_id}, {user_id}, {order_number}, {order_dow}, '{ts}', {days_since_prior})"
                )

                for j in range(1, min(3, records_number + 1)):
                    product_id = 50000 + i
                    orders_products_values.append(
                        f"({order_id}, {product_id}, {j}, {i % 2})"
                    )

            batch_query = f"""
                INSERT INTO orders (order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order)
                VALUES {', '.join(orders_values)};

                INSERT INTO orders_products (order_id, product_id, add_to_cart_order, reordered)
                VALUES {', '.join(orders_products_values)};
            """
            return batch_query
        elif test_name == "update_base":
            return f"""
                UPDATE aisles
                SET aisle = 'Updated Aisle'
                WHERE aisle_id BETWEEN 1 AND {records_number};
            """
        elif test_name == "delete_base":
            return f"""
                DELETE FROM orders 
                WHERE order_id BETWEEN 1 AND {records_number};
            """
        elif test_name == "delete_multi":
            return f"""
                DELETE FROM orders_products 
                WHERE order_id BETWEEN 1 AND {records_number};

                DELETE FROM orders 
                WHERE order_id BETWEEN 1 AND {records_number};
            """

        return ""

    @staticmethod
    def get_mongo_queries(test_name, records_number):
        queries = []

        if test_name == "select_base":
                return ("orders", "find", [{}, {
                    "user_id": 1,
                    "order_dow": 1,
                    "order_number": 1,
                    "order_datetime": 1
                }], records_number)

        elif test_name == "select_join":
                return ("orders", "aggregate", [[
                    # {"$match": {"order_id": i}},
                    {"$unwind": "$products"},
                    {"$lookup": {
                        "from": "products",
                        "localField": "products.product_id",
                        "foreignField": "product_id",
                        "as": "product_details"
                    }},
                    {"$unwind": "$product_details"},
                    {"$project": {
                        "order_id": 1,
                        "user_id": 1,
                        "order_number": 1,
                        "order_datetime": 1,
                        "product_name": "$products.product_name",
                        "aisle_id": "$product_details.aisle_id",
                        "department_id": "$product_details.department_id"
                    }},
                     {"$limit": records_number}
                ]], None)

        elif test_name == "select_date":
            ts = generate_timestamp(1, 1, True)
            query = ("orders", "find", [{
                "order_datetime": {
                    "$gte": datetime.fromtimestamp(ts)
                }
            }, {
                "user_id": 1,
                "order_number": 1,
                "order_datetime": 1
            }], records_number)

            return query

        elif test_name == "insert_base":
            users = []
            for i in range(1, records_number + 1):
                user_id = 300000 + i
                users.append({
                    "user_id": user_id,
                    "name": f"User{user_id}"
                })

            query = ("users", "insert_many", [users], None)
            return query
        
        elif test_name == "insert_multi":
            orders = []

            for i in range(1, records_number + 1):
                ts = generate_timestamp(i % 24, i % 30, True)
                order_id = 5000000 + i
                user_id = 300000 + i

                order = {
                    "order_id": order_id,
                    "user_id": user_id,
                    "order_number": i,
                    "order_dow": i % 7,
                    "order_datetime": datetime.fromtimestamp(ts),
                    "days_since_prior_order": i % 30,
                    "products": []
                }

                for j in range(1, min(3, records_number + 1)):
                    product_id = 50000 + j
                    product = {
                        "product_id": product_id,
                        "product_name": f"Product {chr(64 + j)}",
                        "add_to_cart_order": j,
                        "reordered": i % 2
                    }
                    order["products"].append(product)

                orders.append(order)

            query = ("orders", "insert_many", [orders], None)
            return query

        elif test_name == "update_base":
            query = (
                "aisles",
                "update_many",
                [
                    {"aisle_id": {"$gte": 1, "$lte": records_number}},
                    {"$set": {"aisle": "Updated Aisle"}}
                ],
                None
            )
            return query

        elif test_name == 'delete_base' or test_name == 'delete_mutli':
            query = (
                "orders",
                "delete_many",
                [{"order_id": {"$gte": 1, "$lte": records_number}}],
                None
            )
            return query

        return queries

    @staticmethod
    def get_cassandra_queries(test_name, records_number=1):
        if test_name == "select_base":
            return f"""
                SELECT order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order
                FROM instacart.orders
                LIMIT {records_number}
            """

        elif test_name == "select_join":
            return f"""
                SELECT order_id, user_id, order_number, order_dow, 
                       order_timestamp, product_id, product_name,
                       add_to_cart_order, reordered
                FROM instacart.order_products_by_order
                LIMIT {records_number}
            """

        elif test_name == "select_date":
            ts = generate_timestamp(1, 1)
            return f"""
                SELECT order_id, user_id, order_number, order_timestamp
                FROM instacart.orders_by_timestamp
                WHERE order_timestamp >= '{ts}'
                LIMIT {records_number} ALLOW FILTERING
            """

        elif test_name == "insert_base":
            batch_query = "BEGIN BATCH\n"
            for i in range(1, records_number + 1):
                user_id = 300000 + i
                batch_query += f"""
                    INSERT INTO instacart.users (
                        user_id, name
                    )
                    VALUES (
                        {user_id}, 'User{user_id}'
                    );
                """
            batch_query += "APPLY BATCH;"
            return batch_query

        elif test_name == "insert_multi":
            batch_query = "BEGIN BATCH\n"
            for i in range(1, records_number + 1):
                user_id = 300000 + i
                order_id = 5000000 + i
                order_number = i
                order_dow = i % 7
                ts = generate_timestamp(i % 24, i % 30)
                days_since_prior = i % 30

                batch_query += f"""
                    INSERT INTO instacart.orders (
                        order_id, user_id, order_number, order_dow,
                        order_timestamp, days_since_prior_order
                    )
                    VALUES (
                        {order_id}, {user_id}, {order_number}, {order_dow},
                        '{ts}', {days_since_prior}
                    );
                """

                batch_query += f"""
                    INSERT INTO instacart.orders_by_timestamp (
                        order_timestamp, order_id, user_id, order_number
                    )
                    VALUES (
                        '{ts}', {order_id}, {user_id}, {order_number}
                    );
                """

                for j in range(1, min(3, records_number + 1)):
                    product_id = 50000 + i
                    product_name = f"Product {i}-{j}"
                    batch_query += f"""
                        INSERT INTO instacart.order_products_by_order (
                            order_id, product_id, user_id, order_number, order_dow,
                            order_timestamp, days_since_prior_order,
                            product_name, add_to_cart_order, reordered
                        )
                        VALUES (
                            {order_id}, {product_id}, {user_id}, {order_number}, {order_dow},
                            '{ts}', {days_since_prior},
                            '{product_name}', {j}, {i % 2}
                        );
                    """
            batch_query += "APPLY BATCH;"
            return batch_query

        elif test_name == "update_base":
            batch_query = "BEGIN BATCH\n"

            aisle_ids = [i % 100 for i in range(1, records_number + 1)]
            unique_aisle_ids = list(set(aisle_ids))

            for aisle_id in unique_aisle_ids:
                batch_query += f"""
                    UPDATE instacart.aisles
                    SET aisle = 'Updated Aisle'
                    WHERE aisle_id = {aisle_id};
                """
            batch_query += "APPLY BATCH;"
            return batch_query

        elif test_name == "delete_base":
            batch_query = "BEGIN BATCH\n"

            order_ids = list(range(1, records_number + 1))

            for order_id in order_ids:
                batch_query += f"""
                    DELETE FROM instacart.orders
                    WHERE order_id = {order_id};
                """
            batch_query += "APPLY BATCH;"
            return batch_query

        elif test_name == "delete_multi":
            batch_query = "BEGIN BATCH\n"

            order_ids = list(range(1, records_number + 1))

            for order_id in order_ids:
                batch_query += f"""
                    DELETE FROM instacart.order_products_by_order
                    WHERE order_id = {order_id};
                """
                batch_query += f"""
                    DELETE FROM instacart.orders_by_timestamp
                    WHERE order_id = {order_id};
                """
                batch_query += f"""
                    DELETE FROM instacart.orders
                    WHERE order_id = {order_id};
                """
            batch_query += "APPLY BATCH;"
            return batch_query

        return ""
