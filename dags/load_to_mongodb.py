
from datetime import datetime
import os

import pandas as pd
from pymongo import MongoClient

from airflow.sdk import dag, task, Asset


INPUT_FILE = os.getenv("OUTPUT_FILE")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

COMMENTS_ASSET = Asset("processed_comments")


@dag(
    dag_id="load_to_mongodb",
    description="Upload processed comments to MongoDB",
    start_date=datetime(2026, 11, 1),
    schedule=[COMMENTS_ASSET],
    catchup=False,
    tags=["comments", "mongodb"],
)
def load_to_mongodb():

    @task
    def load_comments():
        df = pd.read_csv(INPUT_FILE)
        records = df.to_dict(orient="records")

        client = MongoClient(MONGODB_URI)

        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]

        collection.delete_many({})

        if records:
            collection.insert_many(records)

        client.close()

    load_comments()


load_to_mongodb()
