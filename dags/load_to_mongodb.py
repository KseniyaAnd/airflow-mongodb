
from datetime import datetime
import os

import pandas as pd
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

from airflow.sdk import dag, task, Asset


INPUT_FILE: str | None = os.getenv("OUTPUT_FILE")
MONGODB_URI: str | None = os.getenv("MONGODB_URI")
MONGODB_DATABASE: str | None = os.getenv("MONGODB_DATABASE")
MONGODB_COLLECTION: str | None = os.getenv("MONGODB_COLLECTION")

COMMENTS_ASSET: Asset = Asset("processed_comments")


@dag(
    dag_id="load_to_mongodb",
    description="Upload processed comments to MongoDB",
    start_date=datetime(2026, 11, 1),
    schedule=[COMMENTS_ASSET],
    catchup=False,
    tags=["comments", "mongodb"],
)
def load_to_mongodb() -> None:
    """
    Load processed comments from a CSV file into MongoDB.

    This DAG is triggered when the processed_comments Asset is updated.
    """

    @task
    def load_comments() -> None:
        """Read processed comments from CSV and insert them into MongoDB."""
        df: pd.DataFrame = pd.read_csv(INPUT_FILE)
        records: list[dict] = df.to_dict(orient="records")

        client: MongoClient = MongoClient(MONGODB_URI)

        db: Database = client[MONGODB_DATABASE]
        collection: Collection = db[MONGODB_COLLECTION]

        collection.delete_many({})

        if records:
            collection.insert_many(records)

        client.close()

    load_comments()


load_to_mongodb()
