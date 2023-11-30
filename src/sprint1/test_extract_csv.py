from extract_csv import read_csv_file
import pytest

# case 1: extract data from csv file to a list of dicts with predefined keys (happy case)
def test_read_csv_file_case_1():
    dummy_file = "../../testing_data/dummy_data.csv"

    expected = [
        {'date_and_time': '25/08/2021 09:21', 
         'city_name': 'Chesterfield', 
         'Customer_Name': 'Tina Burke', 
         'order_details': 'Regular Flavoured iced latte - Vanilla - 2.75, Large Flavoured latte - Hazelnut - 2.85, Large Flavoured latte - Hazelnut - 2.85, Large Latte - 2.45, Regular Flat white - 2.15', 
         'total_amount': '13.05', 
         'payment_type': 'CASH', 
         'Card_Number': ''
         }, 
        {'date_and_time': '25/08/2021 09:23', 
         'city_name': 'Chesterfield', 
         'Customer_Name': 'Robyn Baker', 
         'order_details': 'Regular Latte - 2.15', 
         'total_amount': '2.15', 
         'payment_type': 'CARD', 
         'Card_Number': '4.2844E+15'
         }, 
        {'date_and_time': '25/08/2021 09:25', 
         'city_name': 'Chesterfield', 
         'Customer_Name': 'Leonard Saari', 
         'order_details': 'Large Flat white - 2.45, Large Latte - 2.45, Large Flavoured latte - Hazelnut - 2.85, Regular Flavoured latte - Hazelnut - 2.55', 
         'total_amount': '10.3', 
         'payment_type': 'CARD', 
         'Card_Number': '4.81904E+15'
         }
    ]

    result = read_csv_file(dummy_file)

    assert result == expected


# case 2: if file does not exist, error message return 
def test_read_csv_file_case_2():
    dummy_file = "d.csv" 
    expect_error_message = "Error. File doesn't exist"

    result = read_csv_file(dummy_file)

    assert result == expect_error_message

