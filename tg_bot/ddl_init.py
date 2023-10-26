import psycopg2
import pandas as pd
import os

# Database connection parameters
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
LOAD_DB_PATH = os.environ.get("LOAD_DB_PATH")


if __name__ == "__main__":

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

    # Connect to the database
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()

    # Create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        Name TEXT,
        Quantity FLOAT,
        UnitPrice FLOAT,
        TotalPrice FLOAT,
        Date DATE,
        Shop_name TEXT,
        Category TEXT,
        URL TEXT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

    if LOAD_DB_PATH: 
        cursor = conn.cursor()
        # Load data from CSV into DataFrame
        df = pd.read_csv(LOAD_DB_PATH)

        # Insert data into the table
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO products (Name, Quantity, UnitPrice, TotalPrice, Date, Shop_name, Category, URL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (row["name"], row["quantity"], row["unitprice"], row["totalprice"], row["date"], row["shop_name"], row["category"], row["url"])
            )
        conn.commit()
        cursor.close()

    # Close the connection
    conn.close()