from datetime import datetime

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
    # print('Rows inserted.')