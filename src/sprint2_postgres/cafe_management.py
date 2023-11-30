from sql_script import *
from extract_csv import *
from remove_sensitive_data import *
from update_date_time_format import *
from city_and_payment_tables import *
from products_and_product_flavours_tables import *
from orders_and_products_mapping_tables import *


# MAIN PROGRAM

if __name__ == '__main__':

    connection, cursor = setup_db_connection(database='postgres')

    create_database(connection, cursor, "cafe_management")

    connection.close()

    connection, cursor = setup_db_connection()

    create_database_tables(connection, cursor)   

    sample_data = [] # define a list to store data

    # extract data from csv file
    file_names = ['../../sample-data/leeds_09-05-2023_09-00-00.csv','../../sample-data/chesterfield_25-08-2021_09-00-00.csv','../../sample-data/uppingham_08-08-2023_09-00-00.csv']
    for file in file_names:
        sample_data.extend(read_csv_file(file))

    # remove sensitvie data (i.e. customer name and card number) 
    sample_data = remove_sensitive_data(sample_data)

    # change datatime format into managable

    sample_data = update_date_time_format(sample_data)

    # insert data into city table
    insert_city_table(connection, sample_data)
    
    # # insert data into payment method table
    insert_payment_method_table(connection, sample_data)

    # # insert into products and product_flavours tables
    insert_products_and_product_flavours_tables(connection, sample_data)

    # # insert into orders and products_mapping tables
    insert_orders_and_products_mapping_tables(connection, sample_data)
       
    cursor.close()

    connection.close()
    

    
