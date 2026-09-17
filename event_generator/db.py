import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("POSTGRES-PORT", "5432")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{DB_PORT}/{DB_NAME}")

metadata = MetaData()

metadata.reflect(bind=engine, only=["orders", "order_items", "order_reviews"])

orders = metadata.tables["orders"]
order_items = metadata.tables["order_items"]
order_reviews = metadata.tables["order_reviews"]