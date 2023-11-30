import uuid

def insert_city_table(connection, orders_list):
    print("insert_city_table: starting")
    city_dict = {}
    for order in orders_list:
        city_name = order["city_name"]
        city_dict[city_name] = 1
    
    cursor = connection.cursor()
    for city_name in city_dict:
        city_id = str(uuid.uuid4())  # Generate a GUID for city_id
        print("city_id guid created successfully")
        sql = f"""
        INSERT INTO city (city_id, city_name)
        SELECT * FROM (SELECT '{city_id}', '{city_name}') AS tmp
        WHERE NOT EXISTS (
            SELECT city_name FROM city WHERE city_name = '{city_name}'
        ) LIMIT 1;
        """
        cursor.execute(sql)
    connection.commit()
    cursor.close() 
    print("insert_city_table complete: rows inserted into city table")

def insert_payment_method_table(connection, orders_list):
    print("insert_payment_method_table: starting")
    payment_type={}
    for order in orders_list:
        type = order["payment_type"]
        payment_type[type] = 1

    cursor = connection.cursor()
    for type in payment_type:
        payment_id = str(uuid.uuid4())  # Generate a GUID for payment_id
        print("payment_id guid created successfully")
        sql = f"""
        INSERT INTO payment_method (payment_id, payment_type)
        SELECT * FROM (SELECT '{payment_id}', '{type}') AS tmp
        WHERE NOT EXISTS (
            SELECT payment_type FROM payment_method WHERE payment_type = '{type}'
        ) LIMIT 1;
        """
        cursor.execute(sql)
    connection.commit()
    cursor.close()
    print("insert_payment_method_table complete: rows inserted into payment method table")
