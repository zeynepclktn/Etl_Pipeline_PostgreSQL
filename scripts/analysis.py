import psycopg2
import pandas as pd
from configparser import ConfigParser

def run_sql(cur, conn):
    with open("Fourd Week/etl-pipeline-sql/scripts/analysis.sql", "r") as f:
        query = f.read()
    try:
        cur.execute(query)
        conn.commit()
    except:
        print("The SQL file could not be executed successfully.")

def load_data(cur, conn):
    try:
        with open("Fourd Week/etl-pipeline-sql/data/cleaned_cafe_sales.csv", "r") as f:
            cur.copy_expert("COPY DIRTY_CAFE_SALES FROM STDIN WITH CSV HEADER", f)
        conn.commit()
        print("Data successfully loaded into DIRTY_CAFE_SALES table.")
    except:
        print("An error occurred while loading the data.")

def select_data(conn):
    query = "SELECT * FROM DIRTY_CAFE_SALES"
    df = pd.read_sql(query, conn)
    print("Sample of loaded data:\n", df.head())

def ciro(conn):
    query = "SELECT SUM(total_spent) AS total_revenue FROM dirty_cafe_sales;"
    df = pd.read_sql(query, conn)
    print("📊 Total Revenue:\n", df)

def top_10(conn):
    query = """
    SELECT ID, SUM(quantity) AS total_quantity, SUM(Total_Spent) AS total_revenue
    FROM dirty_cafe_sales
    GROUP BY ID
    ORDER BY total_quantity DESC
    LIMIT 10;
    """
    df = pd.read_sql(query, conn)
    print("🏆 Top 10 Best-Selling Items:\n", df)

def payment_method(conn):
    query = """
    SELECT payment_method, COUNT(*) AS total_transactions, SUM(total_spent) AS total_revenue
    FROM dirty_cafe_sales
    GROUP BY payment_method
    ORDER BY total_revenue DESC;
    """
    df = pd.read_sql(query, conn)
    print("💳 Sales by Payment Method:\n", df)

def location(conn):
    query = """
    SELECT location, COUNT(*) AS total_transactions, SUM(total_spent) AS total_revenue
    FROM dirty_cafe_sales
    GROUP BY location
    ORDER BY total_revenue DESC;
    """
    df = pd.read_sql(query, conn)
    print("📍 Sales by Location:\n", df)

def monthly(conn):
    query = """
    SELECT DATE_TRUNC('month', transaction_date) AS month, SUM(total_spent) AS total_revenue
    FROM dirty_cafe_sales
    GROUP BY month
    ORDER BY month;
    """
    df = pd.read_sql(query, conn)
    print("📅 Monthly Revenue Trend:\n", df)

if __name__ == "__main__":
    config = ConfigParser()
    config.read('Fourd Week/etl-pipeline-sql/pipeline.ini')

    conn = psycopg2.connect(
        dbname=config['postgresql']['dbname'],
        user=config['postgresql']['user'],
        password=config['postgresql']['password'],
        host=config['postgresql']['host'],
        port=config['postgresql']['port']
    )
    cur = conn.cursor()

    run_sql(cur, conn)
    load_data(cur, conn)
    select_data(conn)
    ciro(conn)
    top_10(conn)
    payment_method(conn)
    location(conn)
    monthly(conn)
    conn.commit()
