from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from airflow.sdk import dag, task, Asset

INPUT_FILE = "/opt/airflow/data/processed_comments.csv"

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

        client = MongoClient("mongodb://mongodb:27017/")

        db = client["comments_db"]
        collection = db["comments"]

        collection.delete_many({})

        if records:
            collection.insert_many(records)

        client.close()

    load_comments()


load_to_mongodb()