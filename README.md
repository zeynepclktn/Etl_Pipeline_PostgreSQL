# ETL Pipeline with PostgreSQL

This project demonstrates a simple **ETL (Extract – Transform – Load)** pipeline using Python and PostgreSQL.

## Project Overview

1. **Extract** – Read raw data from CSV files using pandas.
2. **Transform** – Clean and process the data:
   - Handle missing values
   - Convert data types
   - Standardize text fields
3. **Load** – Upload the cleaned data into PostgreSQL.
4. **Analysis** – Run SQL queries to generate summary reports:
   - Top-selling items
   - Revenue by payment method
   - Monthly revenue trends

## Tech Stack

- Python 3
- pandas
- psycopg2
- PostgreSQL

## Project Structure

etl_project/
- data/ # CSV data files
- script/# Script files
- analysis.py # SQL queries for reporting
- transform.py # Data cleaning and transformation
- etl_pipeline.py # Main ETL pipeline
- README.md # Project documentation

## How to Run

1. Install dependencies:

```bash
pip install -r requirements.txt
```
Run the pipeline:
```bash
python pipeline.py
```
Notes
The pipeline is modular; you can replace the CSV with another dataset.
SQL queries generate basic reports for quick analysis.
