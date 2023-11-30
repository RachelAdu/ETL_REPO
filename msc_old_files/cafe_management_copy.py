import csv
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
    cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
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

    create_product_type_table = """
        CREATE TABLE IF NOT EXISTS product_type (
            product_type_id INT NOT NULL AUTO_INCREMENT,
            product_category VARCHAR(50),
            PRIMARY KEY (product_type_id)
        );
    """

    create_product_table = """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT NOT NULL AUTO_INCREMENT,
            flavour_id INT,
            product_type_id INT,
            product_name VARCHAR(50),
            product_size VARCHAR(20),
            product_cost DECIMAL,
            PRIMARY KEY (product_id)
        );
    """

    create_orders_table = """ 
        CREATE TABLE IF NOT EXISTS orders(
            orders_id INT NOT NULL AUTO_INCREMENT,
            city_id INT NOT NULL,
            payment_id INT NOT NULL,
            total_amount DECIMAL,
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
        quantity INT NOT NULL,
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
    cursor.execute(create_product_type_table)
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
            'transaction_time': order['date_and_time'].split()[1]
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
    
    cursor.close()

    connection.close()

    
