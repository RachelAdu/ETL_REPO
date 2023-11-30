import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
host_name = os.environ.get("postgres_host")
database_name = os.environ.get("postgres_db")
user_name = os.environ.get("postgres_user")
user_password = os.environ.get("postgres_password")

def setup_db_connection(host=host_name,
                        user=user_name,
                        password=user_password,
                        database=database_name):


    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    cursor = connection.cursor()
    return connection, cursor

def create_database(connection, cursor, database_name):
    connection.autocommit = True  # Set autocommit to True before creating or dropping the database
    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {database_name};")
        cursor.execute(f"CREATE DATABASE {database_name};")
    except psycopg2.errors.DuplicateDatabase:
        pass
    finally:
        connection.autocommit = False
    connection.commit()
        
def create_database_tables(connection, cursor):

    create_city_table = """
        CREATE TABLE IF NOT EXISTS city (
            city_id VARCHAR(50) NOT NULL,
            city_name VARCHAR(50),
            PRIMARY KEY (city_id)
        );
    """
    
    create_payment_method_table = """
        CREATE TABLE IF NOT EXISTS payment_method (
            payment_id VARCHAR(50) NOT NULL,
            payment_type VARCHAR(50),
            PRIMARY KEY (payment_id)
        );
    """

    create_flavour_table = """
        CREATE TABLE IF NOT EXISTS product_flavours (
            flavour_id VARCHAR(50) NOT NULL,
            flavour_name VARCHAR(50),
            PRIMARY KEY (flavour_id)
        );
    """

    create_product_table = """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) NOT NULL,
            product_name VARCHAR(50),
            product_size VARCHAR(20),
            product_cost DECIMAL(10,2), 
            flavour_id VARCHAR(50),
            city_id VARCHAR(50), 
            PRIMARY KEY (product_id), 
            FOREIGN KEY (flavour_id) REFERENCES product_flavours(flavour_id),
            FOREIGN KEY (city_id) REFERENCES city(city_id)
        );
    """

    create_orders_table = """ 
        CREATE TABLE IF NOT EXISTS orders (
            orders_id VARCHAR(50) NOT NULL,
            city_id VARCHAR(50) NOT NULL,
            payment_id VARCHAR(50) NOT NULL,
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
        orders_id VARCHAR(50) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        order_product_qty INT NOT NULL,
        PRIMARY KEY (orders_id, product_id),
        FOREIGN KEY (orders_id) REFERENCES orders(orders_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """
    print('tables created successfully')

    
    cursor.execute(create_city_table)
    cursor.execute(create_payment_method_table)
    cursor.execute(create_flavour_table)
    cursor.execute(create_product_table)
    cursor.execute(create_orders_table)
    cursor.execute(create_products_mapping_table)
    connection.commit()