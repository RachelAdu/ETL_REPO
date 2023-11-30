def remove_sensitive_data(orders_list):
    for data_dict in orders_list:
        del data_dict['Customer_Name']
        del data_dict['Card_Number']
    return orders_list
