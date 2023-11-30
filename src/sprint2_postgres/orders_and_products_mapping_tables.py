import psycopg2
import uuid

def get_city_id(cursor, city_name):
    print(f"get_city_id: starting")
    try:
        sql = "SELECT city_id FROM city WHERE city_name = %s"
        cursor.execute(sql, (city_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except psycopg2.Error as e:
        print(f"Error getting city_id: {e}")
        return None
    finally:
        print(f"get_city_id: completed")


def get_payment_id(cursor, payment_type):
    print(f"get_payment_id: starting")
    try:
        sql = "SELECT payment_id FROM payment_method WHERE payment_type = %s"
        cursor.execute(sql, (payment_type,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except psycopg2.Error as e:
        print(f"Error getting payment_id: {e}")
        return None
    finally:
        print(f"get_payment_id: completed")


def get_flavour_id(cursor, flavour_name):
    print(f"get_flavour_id: starting")
    try:
        sql = "SELECT flavour_id FROM product_flavours WHERE flavour_name = %s"
        cursor.execute(sql, (flavour_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except psycopg2.Error as e:
        print(f"Error getting flavour_id: {e}")
        return None
    finally:
        print(f"get_flavour_id: completed")


def get_product_id(cursor, product_name, product_size, product_cost, flavour_id, city_id):
    print(f"get_product_id: starting")
    try:
        if flavour_id is None:
            sql = (
                f"SELECT product_id FROM products WHERE product_name = '{product_name}' AND "
                f"product_size = '{product_size}' AND product_cost = {product_cost} AND flavour_id is NULL AND city_id = '{city_id}';"
            )
        else:
            sql = (
                f"SELECT product_id FROM products WHERE product_name = '{product_name}' AND "
                f"product_size = '{product_size}' AND product_cost = {product_cost} AND "
                f"flavour_id = '{flavour_id}' AND city_id = '{city_id}';"
            )
        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except psycopg2.Error as e:
        print(f"Error getting product_id: {e}")
        return None
    finally:
        print(f"get_product_id: completed")


def get_order_id(cursor, city_id, payment_id, total_amount, transaction_date, transaction_time):
    print(f"get_order_id: starting")
    try:
        sql = "SELECT orders_id FROM orders WHERE city_id = %s AND \
                payment_id = %s AND total_amount = %s AND \
                transaction_date = %s AND transaction_time = %s"

        cursor.execute(sql, (city_id, payment_id, total_amount, transaction_date, transaction_time,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except psycopg2.Error as e:
        print(f"Error getting orders_id: {e}")
        return None
    finally:
        print(f"get_order_id: completed")


def get_qty_per_product(list_product_id):
    print(f"get_qty_per_product: starting")
    dict = {}
    for product_id in list_product_id:
        qty = dict.setdefault(product_id, 0)
        dict[product_id] = qty + 1

    list_result = []
    for key, value in dict.items():
        dict_item = {}
        dict_item["product_id"] = key
        dict_item["order_product_qty"] = value
        list_result.append(dict_item)

    print(f"get_qty_per_product: completed")
    return list_result


def prepare_order_products(cursor, messy_order_details, city_id):
    print(f"prepare_order_products: starting")
    list_order_products = []
    list_product_id = []

    list_product_name_flavour_cost = messy_order_details.split(", ")

    for str_product_name_flavour_cost in list_product_name_flavour_cost:
        list_product_name_and_flavour_and_cost_s = str_product_name_flavour_cost.split(" - ")

        product_name = list_product_name_and_flavour_and_cost_s[0]  # product_name
        product_cost = list_product_name_and_flavour_and_cost_s[-1]  # product_cost

        if len(list_product_name_and_flavour_and_cost_s) > 2:
            flavour_name = list_product_name_and_flavour_and_cost_s[-2]
            flavour_id = get_flavour_id(cursor, flavour_name)  # flavour_id
        else:
            flavour_id = None  # flavour_id

        list_temp = []
        list_temp = product_name.split(" ")
        product_size = list_temp[0]  # product_size

        product_id = get_product_id(cursor, product_name, product_size, product_cost, flavour_id, city_id)
        list_product_id.append(product_id)

    list_order_products = get_qty_per_product(list_product_id)

    print(f"prepare_order_products: completed")
    return list_order_products


def prepare_orders_data(cursor, orders_list):
    print(f"prepare_orders_data: starting")
    prepared_orders = []
    for order in orders_list:
        city_name = order['city_name']
        city_id = get_city_id(cursor, city_name)

        payment_type = order['payment_type']
        payment_id = get_payment_id(cursor, payment_type)
        messy_order_details = order['order_details']
        list_order_products = prepare_order_products(cursor, messy_order_details, city_id)

        prepared_order = {
            'city_id': city_id,
            'payment_id': payment_id,
            'total_amount': float(order['total_amount']),
            'transaction_date': order['date_and_time'].split()[0],
            'transaction_time': order['date_and_time'].split()[1],
            'order_products': list_order_products
        }
        prepared_orders.append(prepared_order)
    print(f"prepare_orders_data: completed")
    return prepared_orders


def insert_orders_and_products_mapping_tables(connection, list_orders_data):
    print(f"insert_orders_and_products_mapping_tables: starting")
    try:
        cursor = connection.cursor()

        prepared_orders_data = prepare_orders_data(cursor, list_orders_data)
        for order in prepared_orders_data:
            orders_id = str(uuid.uuid4())  # Generate a GUID for orders_id
            sql_order = f"""
            INSERT INTO orders (orders_id, city_id, payment_id, total_amount, transaction_date, transaction_time)
                SELECT '{orders_id}', '{order['city_id']}', '{order['payment_id']}', {order['total_amount']},
                '{order['transaction_date']}', '{order['transaction_time']}'
                WHERE NOT EXISTS
                    (SELECT orders_id FROM orders WHERE city_id = '{order['city_id']}' AND
                    payment_id = '{order['payment_id']}' AND total_amount = {order['total_amount']} AND
                    transaction_date = '{order['transaction_date']}' AND transaction_time = '{order['transaction_time']}');
            """

            cursor.execute(sql_order)

            order_id = get_order_id(cursor, order['city_id'], order['payment_id'],
                                     order['total_amount'], order['transaction_date'], order['transaction_time'])

            for order_product in order['order_products']:
                sql_order_product = f"""
                INSERT INTO products_mapping_table (orders_id, product_id, order_product_qty)
                    SELECT '{order_id}', '{order_product["product_id"]}', {order_product["order_product_qty"]}
                    WHERE NOT EXISTS
                        (SELECT orders_id, product_id FROM products_mapping_table WHERE
                        orders_id = '{order_id}' AND product_id = '{order_product["product_id"]}')
                """

                cursor.execute(sql_order_product)

        connection.commit()
        cursor.close()
        print('Rows inserted into orders table and products_mapping table.')
    except psycopg2.Error as e:
        print(f"Error inserting data into the orders table: {e}")
        connection.rollback()
    finally:
        print(f"insert_orders_and_products_mapping_tables: completed")