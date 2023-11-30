from sql_script import *
from extract_csv import *
from datetime import datetime
   
# DEFINE FUNCTIONS

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

    cursor.close()

    connection.close()
    
