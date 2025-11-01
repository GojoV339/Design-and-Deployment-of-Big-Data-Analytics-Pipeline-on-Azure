import mysql.connector
from mysql.connector import Error
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

hostname = os.getenv("MYSQL_HOST")
database = os.getenv("MYSQL_DB")
port = os.getenv("MYSQL_PORT")
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")

csv_file_path = "Data/olist_order_payments_dataset.csv"
table_name = "olist_order_payments"

try:
    connection = mysql.connector.connect(
        host=hostname,
        database=database,
        user=username,
        password=password,
        port=port
    )

    if connection.is_connected():
        print("Connected to MySQL")

        cursor = connection.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        print(f"Table {table_name} dropped if it existed.")

        create_table_query = f"""
        CREATE TABLE {table_name} (
            order_id VARCHAR(50),
            payment_sequential INT,
            payment_type VARCHAR(20),
            payment_installments INT,
            payment_value FLOAT
        );
        """

        cursor.execute(create_table_query)
        print(f"Table {table_name} created successfully!")

        data = pd.read_csv(csv_file_path)
        print("CSV loaded into DataFrame.")

        batch_size = 500
        total_records = len(data)

        insert_query = f"""
        INSERT INTO {table_name}
        (order_id, payment_sequential, payment_type, payment_installments, payment_value)
        VALUES (%s, %s, %s, %s, %s);
        """

        print("Starting batch insertion...")

        for start in range(0, total_records, batch_size):
            end = start + batch_size
            batch = data.iloc[start:end]

            batch_records = [tuple(row) for row in batch.itertuples(index=False, name=None)]

            cursor.executemany(insert_query, batch_records)
            connection.commit()

            print(f"Inserted {start+1} → {min(end, total_records)}")

        print(f"✅ All {total_records} rows inserted successfully!")

except Error as e:
    print("Error:", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed.")
