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