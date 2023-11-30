import mysql.connector
import csv
# from load_products_product_flavours_into_db_copy import *
from datetime import datetime
   

import pymysql
import os
from dotenv import load_dotenv

load_dotenv()
host_name = os.environ.get("mysql_host")
database_name = os.environ.get("mysql_db")
user_name = os.environ.get("mysql_user")
user_password = os.environ.get("mysql_pass")

def setup_db_connection(host=host_name, 
                        user=user_name, 
                        password=user_password):

    connection = pymysql.connect(
        host = host,
        user = user,
        password = password
    )
    
    cursor = connection.cursor()
    return connection, cursor

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    cursor.execute(f"USE {db_name};")

def create_database_tables(connection, cursor):

    create_city_table = \
    """
        CREATE TABLE IF NOT EXISTS city(
            city_id INT NOT NULL AUTO_INCREMENT,
            city_name VARCHAR(50),
            PRIMARY KEY (city_id)
        );
    """
    
    create_payment_method_table = """
        CREATE TABLE IF NOT EXISTS payment_method (
            payment_id INT NOT NULL AUTO_INCREMENT,
            payment_type VARCHAR(50),
            PRIMARY KEY (payment_id)
        );
    """

    create_flavour_table = """
        CREATE TABLE IF NOT EXISTS product_flavours (
            flavour_id INT NOT NULL AUTO_INCREMENT,
            flavour_name VARCHAR(50),
            PRIMARY KEY (flavour_id)
        );
    """

    create_product_table = """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT NOT NULL AUTO_INCREMENT,
            product_name VARCHAR(50),
            product_size VARCHAR(20),
            product_cost DECIMAL(10,2), 
            flavour_id INT,
            city_id INT, 
            PRIMARY KEY (product_id), 
            FOREIGN KEY (flavour_id) REFERENCES product_flavours(flavour_id),
            FOREIGN KEY (city_id) REFERENCES city(city_id)
        );
    """

    create_orders_table = """ 
        CREATE TABLE IF NOT EXISTS orders(
            orders_id INT NOT NULL AUTO_INCREMENT,
            city_id INT NOT NULL,
            payment_id INT NOT NULL,
            total_amount DECIMAL(10,2),
            transaction_date DATE,
            transaction_time TIME,
            PRIMARY KEY (orders_id),
            FOREIGN KEY (city_id) REFERENCES city(city_id),
            FOREIGN KEY (payment_id) REFERENCES payment_method(payment_id) 
        );
    """

    create_products_mapping_table = """
    CREATE TABLE IF NOT EXISTS products_mapping_table(
    orders_id INT NOT NULL,
    product_id INT NOT NULL,
  
    PRIMARY KEY (orders_id, product_id),
    FOREIGN KEY (orders_id) REFERENCES orders(orders_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
"""



    create_original_data_table = """
    CREATE TABLE IF NOT EXISTS original_data (
        date_and_time DATETIME, 
        city_name VARCHAR(50),
        order_details VARCHAR(500),
        total_amount DECIMAL,
        payment_type VARCHAR(50)
        );
    """

    cursor.execute(create_city_table)
    cursor.execute(create_payment_method_table)
    cursor.execute(create_flavour_table)
    cursor.execute(create_product_table)
    cursor.execute(create_orders_table)
    cursor.execute(create_products_mapping_table)
    cursor.execute(create_original_data_table)
    connection.commit()



# DEFINE FUNCTIONS

def read_csv_file(file_name):
    sample_data = []
    try:
        with open(file_name, 'r') as file:
            read_file = csv.DictReader(file, fieldnames=['date_and_time', 'city_name', 'Customer_Name', 'order_details', 'total_amount', 'payment_type','Card_Number'], delimiter=',')
            for dict in read_file:
                sample_data.append(dict)
        return sample_data
    except FileNotFoundError:
        return f"Error. File doesn't exist"
    

def remove_sensitive_data(orders_list):
    for data_dict in orders_list:
        del data_dict['Customer_Name']
        del data_dict['Card_Number']
    return orders_list



def update_date_time_format(orders_list):
    for order in orders_list:
        date_time_str = order['date_and_time']
        try:
            date_time_obj = datetime.strptime(date_time_str, '%d/%m/%Y %H:%M')
            date_time_new = date_time_obj.strftime('%Y/%m/%d %H:%M:%S')
            order['date_and_time'] = date_time_new
        except ValueError:
            print(f"Invalid date and time value: {date_time_str}")
    return orders_list


def insert_original_data(connection, orders_list):
    sql = """
        INSERT INTO original_data (date_and_time, city_name, order_details, total_amount, payment_type)
        VALUES (%s, %s, %s, %s, %s);
    """
    cursor = connection.cursor()
    for order in orders_list:
        row = (order['date_and_time'], order['city_name'],
            order['order_details'], order['total_amount'],
            order['payment_type'])
        cursor.execute(sql, row)
              
    connection.commit()
    cursor.close() 
    print('Rows inserted.')


def get_city_id(cursor, city_name):
    try:
        sql = "SELECT city_id FROM city WHERE city_name = %s"
        cursor.execute(sql, (city_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
        
            return None
    except pymysql.Error as e:
        print(f"Error getting city_id: {e}")
        return None

def get_payment_id(cursor, payment_type):
    try:
        sql = "SELECT payment_id FROM payment_method WHERE payment_type = %s"
        cursor.execute(sql, (payment_type,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
          
            return None
    except pymysql.Error as e:
        print(f"Error getting payment_id: {e}")
        return None


def process_orders_data(orders_list):
    processed_orders = []
    for order in orders_list:
        
        city_name = order['city_name']  
        city_id = get_city_id(cursor, city_name)  

        payment_type = order['payment_type']  
        payment_id = get_payment_id(cursor, payment_type)  
        
        processed_order = {
            'city_id': city_id,
            'payment_id': payment_id,
            'total_amount': float(order['total_amount']),
            'transaction_date': order['date_and_time'].split()[0],
            'transaction_time': order['date_and_time'].split()[1],
            'products': []
        }
        processed_orders.append(processed_order)
    return processed_orders


def insert_orders_data(connection, orders_list):
    try:
        cursor = connection.cursor()
        sql = """
            INSERT INTO orders (city_id, payment_id, total_amount, transaction_date, transaction_time)
            VALUES (%s, %s, %s, %s, %s);
        """

        for order in orders_list:
            row = (
                order['city_id'], order['payment_id'],
                order['total_amount'], order['transaction_date'],
                order['transaction_time']
            )
            cursor.execute(sql, row)

        connection.commit()
        cursor.close()
        print('Rows inserted into orders table.')
    except pymysql.Error as e:
        print(f"Error inserting data into the orders table: {e}")
        connection.rollback()

def insert_city_table(connection, orders_list):
    city_dict={}
    for order in orders_list:
        city_name = order["city_name"]
        city_dict[city_name] = 1
    
    cursor = connection.cursor()
    for city_name in city_dict:
        sql = f"""
        INSERT INTO city (city_name)
        SELECT * FROM (SELECT '{city_name}') AS tmp
        WHERE NOT EXISTS (
            SELECT city_name FROM city WHERE city_name = '{city_name}'
        ) LIMIT 1;
        """
        cursor.execute(sql)
    connection.commit()
    cursor.close() 
    print('Rows inserted on city table.')

def insert_payment_method_table(connection, orders_list):
    payment_type={}
    for order in orders_list:
        type = order["payment_type"]
        payment_type[type] = 1

    cursor = connection.cursor()
    for type in payment_type:
        sql = f"""
        INSERT INTO payment_method (payment_type)
        SELECT * FROM (SELECT '{type}') AS tmp
        WHERE NOT EXISTS (
            SELECT payment_type FROM payment_method WHERE payment_type = '{type}'
        ) LIMIT 1;
        """
        cursor.execute(sql)
    connection.commit()
    cursor.close()
    print('Rows inserted on payment method table.')



def transform_to_city_and_product_name_flavour_cost(list_of_dicts):
    list = []

    for dict in list_of_dicts:
        city_name = dict["city_name"]
        product_name_flavour_cost_s = dict["order_details"].split(", ")

        for product_name_flavour_cost in product_name_flavour_cost_s:
            item = {}
            item["city_name"] = city_name
            item["product_name_flavour_cost"] = product_name_flavour_cost
        
            list.append(item)
    
    return list


def transform_to_city_and_product_name_and_flavour_and_cost(list_of_dicts):
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
    
    return list



def transform_to_city_and_product_size_and_name_and_flavour_and_cost(list_of_dicts):

    for dict in list_of_dicts:
        list_temp = []
        list_temp = dict["product_name"].split(" ")
        dict["product_size"] = list_temp[0]

    return list_of_dicts



def transform_messy_to_cities_products_product_flavours(list_of_dicts_messy):

    list_of_dicts_products = transform_to_city_and_product_name_flavour_cost(list_of_dicts_messy)

    list_of_dicts_products = transform_to_city_and_product_name_and_flavour_and_cost(list_of_dicts_products)

    list_of_dicts_products = transform_to_city_and_product_size_and_name_and_flavour_and_cost(list_of_dicts_products)

    return list_of_dicts_products


def get_existing_product_flavours(cursor):

    try:
        sql = "SELECT flavour_name FROM product_flavours ORDER BY flavour_id"
        cursor.execute(sql)
        flavour_names = []
        flavour_names = cursor.fetchall()

        return flavour_names
    
    except Exception as ex:
        print('Failed to def get_existing_product_flavours(cursor):', ex)
        return False  



def load_data_into_product_flavours(connection, list_of_dicts):

    try:
        cursor = connection.cursor()

        tuple_flavour_names = get_existing_product_flavours(cursor)

        sql = """
            INSERT INTO product_flavours (flavour_name)
            VALUES (%s);
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
                value = (flavour_name)
                cursor.execute(sql, value)
                
        connection.commit()
        cursor.close() 

        return True

    except Exception as ex:
        print('Failed to load_data_into_product_flavours:', ex)
        return False  


def get_city_id_by_city_name(cursor, city_name):
    try:
        sql = "SELECT city_id, city_name FROM city ORDER BY city_id"
        cursor.execute(sql)
        cities = []
        cities = cursor.fetchall()

        for city in cities:
            if city_name == city[1]:
                return city[0]
    
    except Exception as ex:
        print('Failed to get_city_id_by_city_nam:', ex)
        return False  
    


def get_flavour_id_by_flavour_name(cursor, flavour_name):
    try:

        sql = "SELECT flavour_id, flavour_name FROM product_flavours ORDER BY flavour_id"
        cursor.execute(sql)
        product_flavours = []
        product_flavours = cursor.fetchall()

        for product_flavour in product_flavours:
            if flavour_name == product_flavour[1]:
                return product_flavour[0]
    
    except Exception as ex:
        print('Failed to get_flavour_id_by_flavour_name:', ex)
        return False  
    


def unify_products(connection, list_of_dicts):
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

        return products

    except Exception as ex:
        print('Failed to unify_products:', ex)
        return False  



def get_existing_products(cursor):

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
        return False  


def get_new_products(tuple_existing_products, tuple_unified_products):

    list_new_products = []
    for tuple_unified in tuple_unified_products:
        is_new_product = True
        for tuple_existing in tuple_existing_products:
            if tuple_unified == tuple_existing:
                is_new_product = False
        if is_new_product is True:
            list_new_products.append(tuple_unified)

    return list_new_products

    

def load_data_into_products(connection, list_of_dicts):

    tuple_unified_products = unify_products(connection, list_of_dicts)

    try:
        cursor = connection.cursor()

        tuple_existing_products = get_existing_products(cursor)

        list_new_products = get_new_products(tuple_existing_products, tuple_unified_products)

        sql_1 = """
            INSERT INTO products (product_name, product_size, product_cost, city_id, flavour_id)
            VALUES (%s, %s, %s, %s, %s);
        """

        sql_2 = """
            INSERT INTO products (product_name, product_size, product_cost, city_id)
            VALUES (%s, %s, %s, %s);
        """

        for list in list_new_products:

            city_id = get_city_id_by_city_name(cursor, list[3])

            if len(list) > 4:
                flavour_id = get_flavour_id_by_flavour_name(cursor, list[4])
            
                values = (list[0], list[1], list[2], city_id, flavour_id)
                cursor.execute(sql_1, values)

            else:
                values = (list[0], list[1], list[2], city_id)
                cursor.execute(sql_2, values)      

        connection.commit()
        cursor.close() 

        return True

    except Exception as ex:
        print('Failed to load_data_into_products:', ex)
        return False  



def load_products_product_flavours_into_db(connection, list_of_dicts_messy, processed_data):
    list_of_dicts = transform_messy_to_cities_products_product_flavours(list_of_dicts_messy)

    if load_data_into_product_flavours(connection, list_of_dicts):
        print("Data loaded to the product_flavours table")

    if load_data_into_products(connection, list_of_dicts):
        print("Data loaded to the products table")
    
    for order in list_of_dicts:
        # Find the corresponding processed order using date and time
        processed_order = next((o for o in processed_data if o['transaction_date'] == order['transaction_date'] and o['transaction_time'] == order['transaction_time']), None)

        if processed_order:
            processed_order['products'].append({
                'product_name': order['product_name'],
                'product_size': order['product_size'],
                'product_cost': order['product_cost'],
                'flavour_name': order['flavour_name']
            })
    return processed_data, list_of_dicts
    



def insert_products_mapping_data(connection, orders_list):
    try:
        cursor = connection.cursor()
        sql = """
            INSERT INTO products_mapping_table (orders_id, product_id)
            VALUES (%s, %s, %s);
        """

        for order in orders_list:
            order_id = get_order_id_by_transaction(cursor, order['transaction_date'], order['transaction_time'])
            for product in order['products']:
                product_id = get_product_id_by_name_and_size(cursor, product['product_name'], product['product_size'])
                row = (order_id, product_id)
                cursor.execute(sql, row)

        connection.commit()
        cursor.close()
        print('Rows inserted into products_mapping_table.')
    except pymysql.Error as e:
        print(f"Error inserting data into the products_mapping_table: {e}")
        connection.rollback()


# Add this function to get the order_id based on transaction date and time
def get_order_id_by_transaction(cursor, transaction_date, transaction_time):
    try:
        sql = "SELECT orders_id FROM orders WHERE transaction_date = %s AND transaction_time = %s"
        cursor.execute(sql, (transaction_date, transaction_time))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except pymysql.Error as e:
        print(f"Error getting order_id: {e}")
        return None
    

# Add this function to get the product_id based on product name and size
def get_product_id_by_name_and_size(cursor, product_name, product_size):
    try:
        sql = "SELECT product_id FROM products WHERE product_name = %s AND product_size = %s"
        cursor.execute(sql, (product_name, product_size))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except pymysql.Error as e:
        print(f"Error getting product_id: {e}")
        return None


# MAIN PROGRAM

if __name__ == '__main__':

    connection, cursor = setup_db_connection()

    create_database(cursor, "cafe_management")

    create_database_tables(connection, cursor)   

    sample_data = [] # define a list to store data

    # extract data from csv file
    sample_data = read_csv_file('../sample-data/leeds_09-05-2023_09-00-00.csv')

    # remove sensitvie data (i.e. customer name and card number) 
    sample_data = remove_sensitive_data(sample_data)

    # change datatime format into managable
    sample_data = update_date_time_format(sample_data)

    # insert data to original_data table which is an intermediate table
    insert_original_data(connection, sample_data)

    insert_city_table(connection, sample_data)

    insert_payment_method_table(connection, sample_data)

    processed_data = process_orders_data(sample_data)
    
    insert_orders_data(connection, processed_data)

    processed_data, list_of_dicts = load_products_product_flavours_into_db(connection, sample_data, processed_data)

    insert_products_mapping_data(connection, processed_data)

    cursor.close()

    connection.close()


# MAIN PROGRAM

# if __name__ == '__main__':

#     connection, cursor = setup_db_connection()

#     create_database(cursor, "cafe_management")

#     create_database_tables(connection, cursor)   

#     sample_data = [] # define a list to store data

#     # extract data from csv file
#     sample_data = read_csv_file('../sample-data/leeds_09-05-2023_09-00-00.csv')

#     # remove sensitvie data (i.e. customer name and card number) 
#     sample_data = remove_sensitive_data(sample_data)

#     # change datatime format into managable
#     sample_data = update_date_time_format(sample_data)

#     # insert data to original_data table which is an intermediate table
#     insert_original_data(connection, sample_data)

#     insert_city_table(connection, sample_data)

#     insert_payment_method_table(connection, sample_data)

#     # transform data and insert into products and product_flavours table
#     load_products_product_flavours_into_db(connection, sample_data)

#     cursor.close()

#     connection.close()