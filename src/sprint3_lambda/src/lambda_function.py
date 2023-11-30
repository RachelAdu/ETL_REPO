import boto3
from sql_script import *
from city_and_payment_tables import *
from products_and_product_flavours_tables import *
from orders_and_products_mapping_tables import *
import csv
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

param_name = 'appuchino_redshift_settings'

def remove_sensitive_data(orders_list):
    for data_dict in orders_list:
        del data_dict['Customer_Name']
        del data_dict['Card_Number']
    return orders_list

def lambda_handler(event, context):
    print(f'lambda_handler: started: event={event}')
    try:
        s3 = boto3.client('s3')
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key= record['s3']['object']['key']
            print(f'lambda_handler: bucket={bucket}, key={key}')
            response = s3.get_object(Bucket=bucket, Key=key)
            body=response['Body'].read().decode('utf-8').split('\n')
            data = csv.DictReader(body, fieldnames=['date_and_time', 'city_name', 'Customer_Name', 'order_details', 'total_amount', 'payment_type','Card_Number'], delimiter=',')
            sample_data=[]
            for dict in data:
                sample_data.append(dict)

        redshift_details = get_ssm_param(param_name)

        connection, cursor = open_sql_database_connection_and_cursor(redshift_details)

        create_database_tables(connection, cursor)

        sample_data = remove_sensitive_data(sample_data)

        sample_data = update_date_time_format(sample_data)

        print(f'lambda_handler: data removed successfully, file={key}')

        insert_city_table(connection, sample_data)

        print(f'lambda_handler: data inserted into city table successfully, file={key}')

        insert_payment_method_table(connection, sample_data)

        print(f'lambda_handler: data inserted into payment method table successfully, file={key}')

        insert_products_and_product_flavours_tables(connection, sample_data)

        print(f'lambda_handler: data inserted into products and product flavours tables successfully, file={key}')

        insert_orders_and_products_mapping_tables(connection, sample_data)

        print(f'lambda_handler: data inserted into orders and products_mapping tables successfully, file={key}')

        print(f'lambda_handler: done, file={key}')

    except Exception as e:
        if key is not None:
            print(f'lambda_handler: error={e}, file={key}')
        else:
            print(f'lambda_handler: error={e}, key is not defined')