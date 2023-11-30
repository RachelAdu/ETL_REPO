import psycopg2 
import boto3
import json

ssm_client = boto3.client('ssm')


def get_ssm_param(param_name):
    print(f'get_ssm_param: getting param_name=${param_name}')
    parameter_details = ssm_client.get_parameter(Name=param_name)
    redshift_details = json.loads(parameter_details['Parameter']['Value'])

    host = redshift_details['host']
    user = redshift_details['user']
    db = redshift_details['database-name']
    print(f'get_ssm_param loaded for db=${db}, user=${user}, host=${host}')
    return redshift_details

def open_sql_database_connection_and_cursor(redshift_details):
    try:
        print('open_sql_database_connection_and_cursor - new connection starting...')
        connection = psycopg2.connect(host=redshift_details['host'],
                                    database=redshift_details['database-name'],
                                    user=redshift_details['user'],
                                    password=redshift_details['password'],
                                    port=redshift_details['port'])
        cursor = connection.cursor()
        print('open_sql_database_connection_and_cursor - connection ready')
        return connection, cursor
    except ConnectionError as e:
        print(f'open_sql_database_connection_and_cursor - failed to open connection:\n{e}')
        raise e
        
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