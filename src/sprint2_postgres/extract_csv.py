#Extract the CSV data

import csv

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




