from datetime_script import generate_timestamp


class DataProvider:
    @staticmethod
    def get_postgres_queries(query_type, num_queries=1):
        queries = []

        if query_type == "select":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    SELECT o.order_id, o.user_id, o.order_number, 
                           p.product_id, p.product_name, p.price, p.department_id,
                           op.add_to_cart_order, op.reordered
                    FROM orders o
                    JOIN order_products op ON o.order_id = op.order_id
                    JOIN products p ON op.product_id = p.product_id
                    WHERE o.order_id = {order_id}
                """)

        elif query_type == "insert":
            for i in range(1, num_queries + 1):
                user_id = 206210 + i
                order_number = i
                order_dow = i % 7
                order_hour = i % 24
                days_since_prior = i % 30
                timestamp = generate_timestamp(order_hour, days_since_prior)

                queries.append(f"""
                    INSERT INTO orders (user_id, order_number, order_dow, order_timestamp, days_since_prior_order)
                    VALUES ({user_id}, {order_number}, {order_dow}, '{timestamp}', {days_since_prior})
                    RETURNING order_id
                """)

        elif query_type == "update":
            for i in range(1, num_queries + 1):
                order_id = i
                timestamp = generate_timestamp(i % 24, i % 30)
                queries.append(f"""
                    UPDATE orders 
                    SET order_timestamp = '{timestamp}'
                    WHERE order_id = {order_id}
                """)

        elif query_type == "delete":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    DELETE FROM orders WHERE order_id = {order_id}
                """)

        return queries

    @staticmethod
    def get_mariadb_queries(query_type, num_queries=1):
        queries = []

        if query_type == "select":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    SELECT o.order_id, o.user_id, o.order_number, 
                           p.product_id, p.product_name, p.price, p.department_id,
                           op.add_to_cart_order, op.reordered
                    FROM orders o
                    JOIN order_products op ON o.order_id = op.order_id
                    JOIN products p ON op.product_id = p.product_id
                    WHERE o.order_id = {order_id}
                """)

        elif query_type == "insert":
            for i in range(1, num_queries + 1):
                user_id = 206210 + i
                order_number = i
                order_dow = i % 7
                order_hour = i % 24
                days_since_prior = i % 30
                timestamp = generate_timestamp(order_hour, days_since_prior)

                queries.append(f"""
                    INSERT INTO orders (user_id, order_number, order_dow, order_timestamp, days_since_prior_order)
                    VALUES ({user_id}, {order_number}, {order_dow}, '{timestamp}', {days_since_prior})
                """)

        elif query_type == "update":
            for i in range(1, num_queries + 1):
                order_id = i
                timestamp = generate_timestamp(i % 24, i % 30)
                queries.append(f"""
                    UPDATE orders 
                    SET order_timestamp = '{timestamp}'
                    WHERE order_id = {order_id}
                """)

        elif query_type == "delete":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    DELETE FROM orders WHERE order_id = {order_id}
                """)

        return queries

    @staticmethod
    def get_mongo_queries(query_type, num_queries=1):
        queries = []

        if query_type == "select":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "find_one", [{"_id": i}, {
                    "user_id": 1,
                    "order_number": 1,
                    "products": 1,
                    "order_timestamp": 1
                }]))

        elif query_type == "insert":
            for i in range(1, num_queries + 1):
                user_id = 206210 + i

                order = {
                    "_id": 4000000 + i,
                    "user_id": user_id,
                    "order_number": i,
                    "order_dow": i % 7,
                    "order_timestamp": generate_timestamp(i % 24, i % 30),
                    "days_since_prior_order": i % 30,
                }
                queries.append(("orders", "insert_one", [order]))

        elif query_type == "update":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "update_one",
                                [{"_id": i}, {"$set": {"order_timestamp": generate_timestamp(i % 24, i % 30)}}]))

        elif query_type == "delete":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "delete_one", [{"_id": i}]))

        return queries

    @staticmethod
    def get_cassandra_queries(query_type, num_queries=1):
        queries = []

        if query_type == "select":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    SELECT order_id, user_id, order_number, order_dow, order_timestamp,
                           product_id, product_name, add_to_cart_order, reordered
                    FROM instacart.order_products_by_order
                    WHERE order_id = {order_id}
                """)

        elif query_type == "insert":
            for i in range(1, num_queries + 1):
                user_id = 206210 + i
                order_id = 4000000 + i
                order_number = i
                order_dow = i % 7
                days_since_prior = i % 30
                timestamp = generate_timestamp(i % 24, i % 30)

                queries.append(f"""
                    INSERT INTO instacart.orders (
                        order_id, user_id, order_number, order_dow, 
                        order_timestamp, days_since_prior_order
                    )
                    VALUES (
                        {order_id}, {user_id}, {order_number}, {order_dow}, 
                        '{timestamp}', {days_since_prior}
                    )
                """)

                for j in range(i % 5 + 1):
                    product_id = 100 + j
                    queries.append(f"""
                        INSERT INTO instacart.order_products_by_order (
                            order_id, product_id, user_id, order_number, order_dow,
                            order_timestamp, days_since_prior_order,
                            product_name, add_to_cart_order, reordered
                        )
                        VALUES (
                            {order_id}, {product_id}, {user_id}, {order_number}, {order_dow},
                            '{timestamp}', {days_since_prior},
                            'Product {product_id}', {j + 1}, {i % 2}
                        )
                    """)

        elif query_type == "update":
            for i in range(1, num_queries + 1):
                order_id = i
                timestamp = generate_timestamp(i % 24, i % 30)

                queries.append(f"""
                    UPDATE instacart.orders 
                    SET order_timestamp = '{timestamp}'
                    WHERE order_id = {order_id}
                """)

                queries.append(f"""
                    UPDATE instacart.order_products_by_order 
                    SET order_timestamp = '{timestamp}'
                    WHERE order_id = {order_id}
                """)

        elif query_type == "delete":
            for i in range(1, num_queries + 1):
                order_id = i

                queries.append(f"""
                    DELETE FROM instacart.order_products_by_order 
                    WHERE order_id = {order_id}
                """)

                queries.append(f"""
                    DELETE FROM instacart.orders 
                    WHERE order_id = {order_id}
                """)

        return queries