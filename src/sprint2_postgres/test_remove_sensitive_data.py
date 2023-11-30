
import pytest
from remove_sensitive_data import *

# Test that the function does remove the 2 keys and their corresponding values
def test_remove_sensitive_data_case_1():
    dummy_data = [
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

    expected = [
        {'date_and_time': '25/08/2021 09:21', 
         'city_name': 'Chesterfield', 
         'order_details': 'Regular Flavoured iced latte - Vanilla - 2.75, Large Flavoured latte - Hazelnut - 2.85, Large Flavoured latte - Hazelnut - 2.85, Large Latte - 2.45, Regular Flat white - 2.15', 
         'total_amount': '13.05', 
         'payment_type': 'CASH', 
         }, 
        {'date_and_time': '25/08/2021 09:23', 
         'city_name': 'Chesterfield', 
         'order_details': 'Regular Latte - 2.15', 
         'total_amount': '2.15', 
         'payment_type': 'CARD', 
         }, 
        {'date_and_time': '25/08/2021 09:25', 
         'city_name': 'Chesterfield', 
         'order_details': 'Large Flat white - 2.45, Large Latte - 2.45, Large Flavoured latte - Hazelnut - 2.85, Regular Flavoured latte - Hazelnut - 2.55', 
         'total_amount': '10.3', 
         'payment_type': 'CARD', 
         }
    ]
    result = remove_sensitive_data(dummy_data)

    assert result == expected


# Test function produces KeyError if the desired keys to remove are not present
def test_remove_sensitive_data_case_2():
    dummy_dict= [
        {'date_and_time': '25/08/2021 09:21', 
         'city_name': 'Chesterfield', 
         'order_details': 'Regular Flavoured iced latte - Vanilla - 2.75, Large Flavoured latte - Hazelnut - 2.85, Large Flavoured latte - Hazelnut - 2.85, Large Latte - 2.45, Regular Flat white - 2.15', 
         'total_amount': '13.05', 
         'payment_type': 'CASH', 
         }, 
        {'date_and_time': '25/08/2021 09:23', 
         'city_name': 'Chesterfield', 
         'order_details': 'Regular Latte - 2.15', 
         'total_amount': '2.15', 
         'payment_type': 'CARD', 
         }, 
        {'date_and_time': '25/08/2021 09:25', 
         'city_name': 'Chesterfield', 
         'order_details': 'Large Flat white - 2.45, Large Latte - 2.45, Large Flavoured latte - Hazelnut - 2.85, Regular Flavoured latte - Hazelnut - 2.55', 
         'total_amount': '10.3', 
         'payment_type': 'CARD', 
         }
    ]

    with pytest.raises(KeyError):
        remove_sensitive_data(dummy_dict)

    #expected = KeyError
    #result = remove_sensitive_data(dummy_dict)
    #assert result == expected