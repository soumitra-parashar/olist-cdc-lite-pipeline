import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
password = os.getenv('POSTGRES_PASSWORD')
engine = create_engine(f'postgresql://olist_admin:{password}@localhost:5432/olist_cdc_db')

for table, filename in [
    ('orders', 'olist_orders_dataset.csv'),
    ('order_items', 'olist_order_items_dataset.csv'),
    ('order_reviews', 'olist_order_reviews_dataset.csv'),
]:
    df = pd.read_csv(f'data/{filename}')
    df.to_sql(table, engine, if_exists='replace', index=False)
    print(f'{table}: {len(df)} rows loaded')
                     