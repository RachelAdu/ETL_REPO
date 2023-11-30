import uuid

def transform_to_city_and_product_name_flavour_cost(list_of_dicts):
    print(f'transform_to_city_and_product_name_flavour_cost: starting')
    
    list = []

    for dict in list_of_dicts:
        city_name = dict["city_name"]
        product_name_flavour_cost_s = dict["order_details"].split(", ")

        for product_name_flavour_cost in product_name_flavour_cost_s:
            item = {}
            item["city_name"] = city_name
            item["product_name_flavour_cost"] = product_name_flavour_cost
        
            list.append(item)

    print(f'transform_to_city_and_product_name_flavour_cost: completed, return tranformed list')
    return list



def transform_to_city_and_product_name_and_flavour_and_cost(list_of_dicts):
    print(f'transform_to_city_and_product_name_and_flavour_and_cost: starting')
    list = []

    for dict in list_of_dicts:
        city_name = dict["city_name"]
        product_name_and_flavour_and_cost_s = dict["product_name_flavour_cost"].split(" - ")

        item = {}
        item["city_name"] = city_name
        item["product_name"] = product_name_and_flavour_and_cost_s[0]
        item["product_cost"] = product_name_and_flavour_and_cost_s[-1]
        if len(product_name_and_flavour_and_cost_s) > 2:
            item["flavour_name"] = product_name_and_flavour_and_cost_s[-2]
        else:
            item["flavour_name"] = None
        list.append(item)
    
    print(f'transform_to_city_and_product_name_and_flavour_and_cost: completed, return transformed list')
    return list



def transform_to_city_and_product_size_and_name_and_flavour_and_cost(list_of_dicts):
    print(f'transform_to_city_and_product_size_and_name_and_flavour_and_cost: starting')

    for dict in list_of_dicts:
        list_temp = []
        list_temp = dict["product_name"].split(" ")
        dict["product_size"] = list_temp[0]

    print(f'transform_to_city_and_product_size_and_name_and_flavour_and_cost: completed, return transformed list')
    return list_of_dicts



def get_existing_product_flavours(cursor):
    print(f'get_existing_product_flavours: starting')

    try:
        sql = "SELECT flavour_name FROM product_flavours ORDER BY flavour_id"
        cursor.execute(sql)
        flavour_names = []
        flavour_names = cursor.fetchall()

        print(f'get_existing_product_flavours: completed, return: {flavour_names}')
        return flavour_names
    
    except Exception as ex:
        print('Failed to def get_existing_product_flavours(cursor):', ex)

        print(f'get_existing_product_flavours: completed (with error), return False')
        return False  



def load_data_into_product_flavours(connection, list_of_dicts):
    print(f'load_data_into_product_flavours: starting')

    try:
        cursor = connection.cursor()

        tuple_flavour_names = get_existing_product_flavours(cursor)

        sql = """
            INSERT INTO product_flavours (flavour_id, flavour_name)
            VALUES (%s, %s);
        """

        # collect all flavour_names to a list
        list_flavour_names = []
        for dict in list_of_dicts:
            if "flavour_name" in dict.keys():
                is_exist_in_db = False
                for tuple in tuple_flavour_names:
                    if dict["flavour_name"] == tuple[0]:
                        is_exist_in_db = True
                if is_exist_in_db is not True:
                    list_flavour_names.append(dict["flavour_name"])

        # unify flavour_names
        set_flavour_names = set(list_flavour_names)

        # insert into product_flavours table
        for flavour_name in set_flavour_names:
            if flavour_name is not None:
                flavour_id = str(uuid.uuid4())  # Generate a GUID for flavour_id
                value = (flavour_id, flavour_name)
                cursor.execute(sql, value)
                
        connection.commit()
        cursor.close() 

        print(f'load_data_into_product_flavours: completed, return: True')
        return True

    except Exception as ex:
        print('Failed to load_data_into_product_flavours:', ex)

        print(f'load_data_into_product_flavours: completed (with error), return: False')
        return False  


def get_city_id_by_city_name(cursor, city_name):
    print(f'get_city_id_by_city_name: starting, city_name: {city_name}')
    try:
        sql = "SELECT city_id, city_name FROM city ORDER BY city_id"
        cursor.execute(sql)
        cities = []
        cities = cursor.fetchall()

        for city in cities:
            if city_name == city[1]:
                
                print(f'get_city_id_by_city_name: completed, return: {city[0]}')

                return city[0]
    
    except Exception as ex:
        print('Failed to get_city_id_by_city_nam:', ex)

        print(f'get_city_id_by_city_name: completed (with error), return: False')
        return False  
    


def get_flavour_id_by_flavour_name(cursor, flavour_name):

    print(f'get_flavour_id_by_flavour_name: starting, flavour_name: {flavour_name}')
    try:

        sql = "SELECT flavour_id, flavour_name FROM product_flavours ORDER BY flavour_id"
        cursor.execute(sql)
        product_flavours = []
        product_flavours = cursor.fetchall()

        for product_flavour in product_flavours:
            if flavour_name == product_flavour[1]:

                print(f'get_flavour_id_by_flavour_name: completed, return: {product_flavour[0]}')

                return product_flavour[0]
    
    except Exception as ex:
        print('Failed to get_flavour_id_by_flavour_name:', ex)

        print(f'get_flavour_id_by_flavour_name: completed (with error), return: False')

        return False  
    


def unify_products(connection, list_of_dicts):
    print(f'unify_products: starting')
    try:
        cursor = connection.cursor()

        cursor.execute("DROP TABLE IF EXISTS temp_product_data;")

        create_temp_product_data_table = """
            CREATE TABLE IF NOT EXISTS temp_product_data (
                product_name VARCHAR(50),
                product_size VARCHAR(20),
                product_cost DECIMAL(10,2), 
                city_name VARCHAR(50),
                flavour_name VARCHAR(50)
            );
    """
        cursor.execute(create_temp_product_data_table)

        sql_1 = """
            INSERT INTO temp_product_data (product_name, product_size, product_cost, city_name, flavour_name)
            VALUES (%s, %s, %s, %s, %s);
        """

        sql_2 = """
            INSERT INTO temp_product_data (product_name, product_size, product_cost, city_name)
            VALUES (%s, %s, %s, %s);
        """

        for dict in list_of_dicts:

            if "flavour_name" in dict.keys():
            
                values = (dict['product_name'], dict['product_size'],
                    dict['product_cost'], dict['city_name'], dict['flavour_name'])
                cursor.execute(sql_1, values)

            else:
                values = (dict['product_name'], dict['product_size'],
                    dict['product_cost'], dict['city_name'])
                cursor.execute(sql_2, values)     


        sql = "SELECT DISTINCT product_name, product_size, product_cost, city_name, flavour_name \
            FROM temp_product_data ORDER BY city_name, product_name"
        cursor.execute(sql)
        products = []
        products = cursor.fetchall()

        cursor.execute("DROP TABLE IF EXISTS temp_product_data;") # drop temp table

        connection.commit()
        cursor.close() 

        print(f'unify_products: completed')
        return products

    except Exception as ex:
        print('Failed to unify_products:', ex)

        print(f'unify_products: completed (with error), return False')

        return False  



def get_existing_products(cursor):
    print(f'get_existing_products: starting')

    try:
        sql = "SELECT p.product_name, p.product_size, \
                p.product_cost, c.city_name, f.flavour_name  \
                FROM products as p \
                INNER JOIN city as c \
                ON p.city_id = c.city_id \
                INNER JOIN product_flavours as f \
                on p.flavour_id = f.flavour_id \
                UNION \
                SELECT p.product_name, p.product_size, \
                p.product_cost, c.city_name, NULL as flavour_name \
                FROM products as p \
                INNER JOIN city as c \
                ON p.city_id = c.city_id \
                WHERE p.flavour_id IS NULL;"

        cursor.execute(sql)
        products = []
        products = cursor.fetchall()

        return products
    
    except Exception as ex:
        print('Failed to def get_existing_products(cursor):', ex)

        print(f'get_existing_products: completed (with error), return False')
        return False  


def get_new_products(tuple_existing_products, tuple_unified_products):

    print(f'get_new_products: starting')
    list_new_products = []
    for tuple_unified in tuple_unified_products:
        is_new_product = True
        for tuple_existing in tuple_existing_products:
            if tuple_unified == tuple_existing:
                is_new_product = False
        if is_new_product is True:
            list_new_products.append(tuple_unified)

    print(f'get_new_products: completed')
    return list_new_products

    

def load_data_into_products(connection, list_of_dicts):
    print(f'load_data_into_products: starting')

    tuple_unified_products = unify_products(connection, list_of_dicts)

    try:
        cursor = connection.cursor()

        tuple_existing_products = get_existing_products(cursor)

        list_new_products = get_new_products(tuple_existing_products, tuple_unified_products)

        sql_1 = """
            INSERT INTO products (product_id, product_name, product_size, product_cost, city_id, flavour_id)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        sql_2 = """
            INSERT INTO products (product_id, product_name, product_size, product_cost, city_id)
            VALUES (%s, %s, %s, %s, %s);
        """

        for list in list_new_products:

            city_id = get_city_id_by_city_name(cursor, list[3])
            product_id = str(uuid.uuid4())  # Generate a GUID for product_id

            if len(list) > 4:
                flavour_id = get_flavour_id_by_flavour_name(cursor, list[4])
            
                values = (product_id, list[0], list[1], list[2], city_id, flavour_id)
                cursor.execute(sql_1, values)

            else:
                values = (product_id, list[0], list[1], list[2], city_id)
                cursor.execute(sql_2, values)      

        connection.commit()
        cursor.close() 

        print(f'load_data_into_products: completed, return True')
        return True

    except Exception as ex:
        print('Failed to load_data_into_products:', ex)

        print(f'load_data_into_products: completed (with error), return False')

        return False  



def transform_messy_to_cities_products_product_flavours(list_of_dicts_messy):
    print(f'transform_messy_to_cities_products_product_flavours: starting')

    list_of_dicts_products = transform_to_city_and_product_name_flavour_cost(list_of_dicts_messy)

    list_of_dicts_products = transform_to_city_and_product_name_and_flavour_and_cost(list_of_dicts_products)

    list_of_dicts_products = transform_to_city_and_product_size_and_name_and_flavour_and_cost(list_of_dicts_products)

    print(f'transform_messy_to_cities_products_product_flavours: completed')
    return list_of_dicts_products



def insert_products_and_product_flavours_tables(connection, list_of_dicts_messy):
    print(f'insert_products_and_product_flavours_tables: starting')

    list_of_dicts = transform_messy_to_cities_products_product_flavours(list_of_dicts_messy)

    if load_data_into_product_flavours(connection, list_of_dicts):
        print("Rows inserted into product_flavours table.")

    if load_data_into_products(connection, list_of_dicts):
        print("Rows inserted into products table.")

    print(f'insert_products_and_product_flavours_tables: completed')
