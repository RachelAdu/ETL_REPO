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

