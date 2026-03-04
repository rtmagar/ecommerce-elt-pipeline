import os
import pandas as pd
from sqlalchemy import create_engine

# Database connection string for the OLTP database (matching docker-compose)
DB_USER = "admin"
DB_PASS = "admin_password"
DB_HOST = "localhost" # Assuming you run this script on your host machine
DB_PORT = "5433"
DB_NAME = "olist_production"

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Mapping of the exact Kaggle CSV filenames to our desired table names
csv_to_table_mapping = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation",
}

RAW_DATA_DIR = "./raw_data"

def load_data():
    print("Starting initial data load into OLTP Database...")
    
    for csv_file, table_name in csv_to_table_mapping.items():
        file_path = os.path.join(RAW_DATA_DIR, csv_file)
        
        if not os.path.exists(file_path):
            print(f"WARNING: File {file_path} not found. Skipping {table_name}.")
            continue
            
        print(f"Reading {csv_file}...")
        # Read CSV into Pandas DataFrame
        df = pd.read_csv(file_path)
        
        print(f"Loading {len(df)} rows into table '{table_name}'...")
        # Write the data to Postgres. 'replace' drops the table if it exists and recreates it.
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"Successfully loaded {table_name}.\n")

    print("Initial OLTP database setup complete!")

if __name__ == "__main__":
    load_data()
