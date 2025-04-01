class DataProvider:
    @staticmethod
    def get_postgres_queries(query_type, num_queries=1):
        """Generate PostgreSQL queries based on the query type and number of queries."""
        queries = []

        if query_type == "select":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    SELECT o.order_id, o.user_id, o.order_number, 
                           op.product_id, p.product_name, p.aisle_id, p.department_id
                    FROM orders o
                    JOIN order_products op ON o.order_id = op.order_id
                    JOIN products p ON op.product_id = p.product_id
                    WHERE o.order_id = {order_id}
                """)

        elif query_type == "insert":
            for i in range(1, num_queries + 1):
                user_id = 10000 + i
                order_number = i
                order_dow = i % 7
                order_hour = i % 24
                days_since_prior = i % 30

                queries.append(f"""
                    INSERT INTO orders (user_id, order_number, order_dow, order_hour_of_day, days_since_prior_order)
                    VALUES ({user_id}, {order_number}, {order_dow}, {order_hour}, {days_since_prior})
                    RETURNING order_id
                """)

        elif query_type == "update":
            for i in range(1, num_queries + 1):
                order_id = i
                new_hour = (i % 24)
                queries.append(f"""
                    UPDATE orders 
                    SET order_hour_of_day = {new_hour}
                    WHERE order_id = {order_id}
                """)

        elif query_type == "delete":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    DELETE FROM order_products WHERE order_id = {order_id}
                """)
                queries.append(f"""
                    DELETE FROM orders WHERE order_id = {order_id}
                """)

        return queries

    @staticmethod
    def get_mariadb_queries(query_type, num_queries=1):
        """Generate MariaDB queries based on the query type and number of queries."""
        return DataProvider.get_postgres_queries(query_type, num_queries)

    @staticmethod
    def get_mongo_queries(query_type, num_queries=1):
        """Generate MongoDB operations based on the query type and number of queries."""
        queries = []

        if query_type == "select":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "find_one", [{"_id": i}]))

        elif query_type == "insert":
            for i in range(1, num_queries + 1):
                user_id = 10000 + i
                order = {
                    "_id": 1000000 + i,
                    "user_id": user_id,
                    "order_number": i,
                    "order_dow": i % 7,
                    "order_hour_of_day": i % 24,
                    "days_since_prior_order": i % 30,
                    "products": [
                        {
                            "product_id": 100 + (i % 10),
                            "add_to_cart_order": 1,
                            "reordered": 0
                        }
                    ]
                }
                queries.append(("orders", "insert_one", [order]))

        elif query_type == "update":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "update_one",
                                [{"_id": i}, {"$set": {"order_hour_of_day": i % 24}}]))

        elif query_type == "delete":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "delete_one", [{"_id": i}]))

        return queries

    @staticmethod
    def get_cassandra_queries(query_type, num_queries=1):
        """Generate Cassandra queries based on the query type and number of queries."""
        queries = []

        if query_type == "select":
            for i in range(1, num_queries + 1):
                queries.append(f"SELECT * FROM instacart.orders WHERE order_id = {i}")
                queries.append(f"SELECT * FROM instacart.order_products WHERE order_id = {i}")

        elif query_type == "insert":
            for i in range(1, num_queries + 1):
                user_id = 10000 + i
                order_id = 1000000 + i
                queries.append(f"""
                    INSERT INTO instacart.orders (
                        order_id, user_id, order_number, order_dow, 
                        order_hour_of_day, days_since_prior_order
                    )
                    VALUES (
                        {order_id}, {user_id}, {i}, {i % 7}, 
                        {i % 24}, {i % 30}
                    )
                """)

                product_id = 100 + (i % 10)
                queries.append(f"""
                    INSERT INTO instacart.order_products (
                        order_id, product_id, add_to_cart_order, reordered
                    )
                    VALUES (
                        {order_id}, {product_id}, 1, 0
                    )
                """)

        elif query_type == "update":
            for i in range(1, num_queries + 1):
                queries.append(f"""
                    UPDATE instacart.orders 
                    SET order_hour_of_day = {i % 24}
                    WHERE order_id = {i}
                """)

        elif query_type == "delete":
            for i in range(1, num_queries + 1):
                queries.append(f"DELETE FROM instacart.order_products WHERE order_id = {i}")
                queries.append(f"DELETE FROM instacart.orders WHERE order_id = {i}")

        return queries