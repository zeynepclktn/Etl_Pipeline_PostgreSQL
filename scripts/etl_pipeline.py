import psycopg2
import transform
import analysis

def run_pipeline():
    print("🚀 Starting ETL Pipeline...")

    # 1. Transform Step
    print("🔧 Transforming raw data...")
    transform.main()  # transform.py'de __main__ yerine fonksiyon tanımlamanı öneririm

    # 2. PostgreSQL Connection
    print("🔗 Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        dbname="week3",
        user="postgres",
        password="130699",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # 3. Load Step (create table + load cleaned data)
    print("📥 Loading cleaned data into PostgreSQL...")
    analysis.run_sql(cur, conn)
    analysis.load_data(cur, conn)

    # 4. Reporting Step
    print("📊 Running analysis reports...")
    analysis.select_data(conn)
    analysis.ciro(conn)
    analysis.top_10(conn)
    analysis.payment_method(conn)
    analysis.location(conn)
    analysis.monthly(conn)

    conn.commit()
    conn.close()
    print("✅ ETL Pipeline completed successfully!")

if __name__ == "__main__":
    run_pipeline()
