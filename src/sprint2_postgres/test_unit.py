import unittest
import psycopg2
from city_and_payment_tables import *
from dotenv import load_dotenv
import os
from orders_and_products_mapping_tables import *

load_dotenv()
host_name = os.environ.get("postgres_host")
database_name = os.environ.get("postgres_db")
user_name = os.environ.get("postgres_user")
user_password = os.environ.get("postgres_password")


def setup_db_connection(host=host_name,
                        user=user_name,
                        password=user_password,
                        database=database_name):
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    cursor = connection.cursor()
    return connection, cursor


class TestInsertCityTable(unittest.TestCase):

    def test_insert_city_table(self):
        conn, cursor = setup_db_connection()

        test_city_names = [
            {"city_name": "Leeds"},
            {"city_name": "Chesterfield"},
            {"city_name": "Leeds"}
        ]
        insert_city_table(conn, test_city_names)

        cursor = conn.cursor()
        cursor.execute("SELECT city_name FROM city ORDER BY city_name")
        result = cursor.fetchall()

        expected = [('Chesterfield',), ('Leeds',)]

        self.assertEqual(result, expected)
        cursor.execute("DELETE FROM city")
        conn.commit()
        conn.close()

    def test_insert_payment_table(self):
        conn, cursor = setup_db_connection()

        test_payment_type = [
            {"payment_type": "CASH"},
            {"payment_type": "CARD"},
            {"payment_type": "CASH"}
        ]
        insert_payment_method_table(conn, test_payment_type)

        cursor = conn.cursor()
        cursor.execute("SELECT payment_type FROM payment_method ORDER BY payment_type")
        result = cursor.fetchall()

        expected = [('CARD',), ('CASH',)]

        self.assertEqual(result, expected)
        cursor.execute("DELETE FROM payment_method")
        conn.commit()
        conn.close()

    def test_get_city_id_happy_path(self):
        conn, cursor = setup_db_connection()

        test_city_name = "Leeds"
        test_city_id = str(uuid.uuid4())
        cursor.execute("INSERT INTO city (city_id, city_name) VALUES (%s, %s)", (test_city_id, test_city_name))
        conn.commit()

        city_id = get_city_id(cursor, test_city_name)

        self.assertEqual(city_id, test_city_id)
        cursor.execute("DELETE FROM city")
        conn.commit()
        conn.close()

    def test_get_city_id_unhappy_path(self):
        conn, cursor = setup_db_connection()

        test_city_name = "Appuccino"

        city_id = get_city_id(cursor, test_city_name)

        self.assertIsNone(city_id)
        conn.close()
