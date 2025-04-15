from datetime_script import generate_timestamp
from datetime import datetime

class DataProvider:
    @staticmethod
    def get_postgres_queries(test_name, num_queries=1):
        queries = []

        if test_name == "select":
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

        elif test_name == "insert":
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

        elif test_name == "update":
            for i in range(1, num_queries + 1):
                order_id = i
                timestamp = generate_timestamp(i % 24, i % 30)
                queries.append(f"""
                    UPDATE orders 
                    SET order_timestamp = '{timestamp}'
                    WHERE order_id = {order_id}
                """)

        elif test_name == "delete":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    DELETE FROM orders WHERE order_id = {order_id}
                """)

        return queries

    @staticmethod
    def get_mariadb_queries(test_name, num_queries=1):
        queries = []

        if test_name == "select":
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

        elif test_name == "insert":
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

        elif test_name == "update":
            for i in range(1, num_queries + 1):
                order_id = i
                timestamp = generate_timestamp(i % 24, i % 30)
                queries.append(f"""
                    UPDATE orders 
                    SET order_timestamp = '{timestamp}'
                    WHERE order_id = {order_id}
                """)

        elif test_name == "delete":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    DELETE FROM orders WHERE order_id = {order_id}
                """)

        return queries

    @staticmethod
    def get_mongo_queries(test_name, num_queries=1):
        queries = []

        if test_name == "select_base":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "find_one", [{"order_id": i}, {
                    "user_id": 1,
                    "order_dow": 1,
                    "order_number": 1,
                    "order_datetime": 1
                }]))


        elif test_name == "select_join":
            for i in range(1, num_queries + 1):
                queries.append(("orders", "aggregate", [[
                    {"$match": {"order_id": i}},
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
                    }}
                ]]))


        elif test_name == "select_date":
            for i in range(1, num_queries + 1):
                ts = generate_timestamp((i % 24), (i % 30), True)
                queries.append(("orders", "find_one", [{
                    "order_datetime": {
                        "$gte": datetime.fromtimestamp(ts)
                    }
                }, {
                    "user_id": 1,
                    "order_number": 1,
                    "order_datetime": 1
                }]))

        elif test_name == "insert_base":
            for i in range(1, num_queries + 1):
                user = {
                    "user_id": 300000 + i,
                    "name": f"User{300000 + i}"
                }
                queries.append(("users", "insert_one", [user]))

        elif test_name == "insert_multi":
            for i in range(1, num_queries + 1):
                ts = generate_timestamp((i % 24), (i % 30), True)
                order_id = 5000000 + i
                user_id = 300000 + i
                order = {
                    "order_id": order_id,
                    "user_id": user_id,
                    "order_number": i,
                    "order_dow": i % 7,
                    "order_datetime": datetime.fromtimestamp(ts),
                    "products": [
                        {"product_id": 50001, "product_name": "Product A"},
                        {"product_id": 50002, "product_name": "Product B"}
                    ]
                }
                queries.append(("orders", "insert_one", [order]))
        
        elif test_name == "update_base":
            for i in range(1, num_queries + 1):
                queries.append(("aisles", "update_one", [
                    {"aisle_id": i % 100},
                    {"$set": {"aisle": f"Updated Aisle {i % 7}"}}
                ]))

        elif test_name == 'delete_base' or test_name == 'delete_mutli':
            for i in range(1, num_queries + 1):
                queries.append(("orders", "delete_one", [
                    {"order_id": i}
                ]))

        # elif test_name == "insert":
        #     for i in range(1, num_queries + 1):
        #         user_id = 206210 + i

        #         order = {
        #             "_id": 4000000 + i,
        #             "user_id": user_id,
        #             "order_number": i,
        #             "order_dow": i % 7,
        #             "order_timestamp": generate_timestamp(i % 24, i % 30),
        #             "days_since_prior_order": i % 30,
        #         }
        #         queries.append(("orders", "insert_one", [order]))

        # elif test_name == "update":
        #     for i in range(1, num_queries + 1):
        #         queries.append(("orders", "update_one",
        #                         [{"_id": i}, {"$set": {"order_timestamp": generate_timestamp(i % 24, i % 30)}}]))

        # elif test_name == "delete":
        #     for i in range(1, num_queries + 1):
        #         queries.append(("orders", "delete_one", [{"_id": i}]))

        return queries

    @staticmethod
    def get_cassandra_queries(test_name, num_queries=1):
        queries = []

        if test_name == "select_base":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    SELECT order_id, user_id, order_number, order_dow, order_timestamp, days_since_prior_order
                    FROM instacart.orders
                    WHERE order_id = {order_id}
                """)

        elif test_name == "select_join":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    SELECT order_id, user_id, order_number, order_dow, 
                           order_timestamp, product_id, product_name,
                           add_to_cart_order, reordered
                    FROM instacart.order_products_by_order
                    WHERE order_id = {order_id}
                """)

        elif test_name == "select_date":
            for i in range(1, num_queries + 1):
                ts = generate_timestamp(i % 24, i % 30)
                queries.append(f"""
                    SELECT order_id, user_id, order_number, order_timestamp
                    FROM instacart.orders_by_timestamp
                    WHERE order_timestamp >= '{ts}'
                    LIMIT 1
                """)

        elif test_name == "insert_base":
            for i in range(1, num_queries + 1):
                user_id = 300000 + i
                queries.append(f"""
                    INSERT INTO instacart.users (
                        user_id, name
                    )
                    VALUES (
                        {user_id}, 'User{user_id}'
                    )
                """)

        elif test_name == "insert_multi":
            for i in range(1, num_queries + 1):
                user_id = 300000 + i
                order_id = 5000000 + i
                order_number = i
                order_dow = i % 7
                ts = generate_timestamp(i % 24, i % 30)
                days_since_prior = i % 30

                queries.append(f"""
                    INSERT INTO instacart.orders (
                        order_id, user_id, order_number, order_dow,
                        order_timestamp, days_since_prior_order
                    )
                    VALUES (
                        {order_id}, {user_id}, {order_number}, {order_dow},
                        '{ts}', {days_since_prior}
                    )
                """)

                queries.append(f"""
                    INSERT INTO instacart.orders_by_timestamp (
                        order_timestamp, order_id, user_id, order_number
                    )
                    VALUES (
                        '{ts}', {order_id}, {user_id}, {order_number}
                    )
                """)

                for j in range(1, 3):
                    product_id = 50000 + i
                    product_name = f"Product {i}-{j}"
                    queries.append(f"""
                        INSERT INTO instacart.order_products_by_order (
                            order_id, product_id, user_id, order_number, order_dow,
                            order_timestamp, days_since_prior_order,
                            product_name, add_to_cart_order, reordered
                        )
                        VALUES (
                            {order_id}, {product_id}, {user_id}, {order_number}, {order_dow},
                            '{ts}', {days_since_prior},
                            '{product_name}', {j}, {i % 2}
                        )
                    """)

        elif test_name == "update_base":
            for i in range(1, num_queries + 1):
                aisle_id = i % 100
                queries.append(f"""
                    UPDATE instacart.aisles
                    SET aisle = 'Updated Aisle {i % 7}'
                    WHERE aisle_id = {aisle_id}
                """)

        elif test_name == "delete_base":
            for i in range(1, num_queries + 1):
                order_id = i
                queries.append(f"""
                    DELETE FROM instacart.orders
                    WHERE order_id = {order_id}
                """)

        elif test_name == "delete_multi":
            for i in range(1, num_queries + 1):
                order_id = i

                queries.append(f"""
                    DELETE FROM instacart.products
                    WHERE product_id IN (
                        SELECT product_id 
                        FROM instacart.order_products_by_order 
                        WHERE order_id = {order_id}
                    )
                """)

                queries.append(f"""
                    DELETE FROM instacart.order_products_by_order
                    WHERE order_id = {order_id}
                """)

                queries.append(f"""
                    DELETE FROM instacart.orders_by_timestamp
                    WHERE order_id = {order_id}
                """)

                queries.append(f"""
                    DELETE FROM instacart.orders
                    WHERE order_id = {order_id}
                """)

        return queries
