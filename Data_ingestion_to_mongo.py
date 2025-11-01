import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

product_category_df = pd.read_csv("Data/product_category_name_translation.csv")

uri = os.getenv("MONGO_URI")

client = None

try:
    client = MongoClient(uri)
    db = client["MongoDB_laiddoneto"]
    collection_name = "product_categories"

    if collection_name in db.list_collection_names():
        db[collection_name].drop()

    collection = db[collection_name]
    data_to_insert = product_category_df.to_dict(orient="records")
    collection.insert_many(data_to_insert)

    print("Data uploaded successfully")
except Exception as e:
    print("Error:", e)
finally:
    if client:
        client.close()
