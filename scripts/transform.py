import pandas as pd

def info_data(df):
    print(df.info())
    print(df.describe(include='all'))
    print(df.isnull().sum())

def fix_values(df):
    df['Item'] = df['Item'].replace('ERROR', 'UNKNOWN')
    df['Item'] = df['Item'].fillna('UNKNOWN')
    df['Item'] = df['Item'].str.strip().str.title()

def convert_data(df):
    # Convert to numeric, coercing errors to NaN
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    df['Price Per Unit'] = pd.to_numeric(df['Price Per Unit'], errors='coerce')
    df['Total Spent'] = pd.to_numeric(df['Total Spent'], errors='coerce')

    # Fill missing values using logical calculations
    df['Price Per Unit'] = df['Price Per Unit'].fillna((df['Total Spent'] / df['Quantity']).round(2))
    df['Total Spent'] = df['Total Spent'].fillna((df['Price Per Unit'] * df['Quantity']).round(2))
    df['Quantity'] = df['Quantity'].fillna((df['Total Spent'] / df['Price Per Unit']).round(2))

    # Fill any remaining missing values with 'Unknown'
    df['Price Per Unit'] = pd.to_numeric(df['Price Per Unit'], errors='coerce')
    df['Total Spent'] = pd.to_numeric(df['Total Spent'], errors='coerce')
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')


def fix_date(df):
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')
    mode_date = df['Transaction Date'].mode().iat[0]
    df['Transaction Date'] = df['Transaction Date'].fillna(mode_date)

def clean_data(df,column_name):
    df[column_name] = df[column_name].str.strip().str.title().fillna('Unknown')
    df[column_name] = df[column_name].replace('Error', 'Unknown')
    df = df.replace("Unknown", None)

def load_csv(df):
    df.to_csv("Fourd Week/etl-pipeline-sql/data/cleaned_cafe_sales.csv", index=False)

def main():
    df = pd.read_csv("Fourd Week/etl-pipeline-sql/data/dirty_cafe_sales.csv")
    info_data(df)
    fix_values(df)
    convert_data(df)
    fix_date(df)
    clean_data(df,'Payment Method')
    clean_data(df,'Location')
    info_data(df)

    