from datetime import datetime
import os
import re

import pandas as pd

from airflow.sdk import dag, task, Asset
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


INPUT_FILE = "/opt/airflow/data/comments.csv"

OUTPUT_FILE = "/opt/airflow/data/processed_comments.csv"

COMMENTS_ASSET = Asset("processed_comments")


@dag(
    dag_id="comments_pipeline",
    description="Waiting and updating comments from CSV",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["comments", "pandas"],
)
def comments_pipeline():

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    @task.branch
    def check_file():
        if os.path.getsize(INPUT_FILE) == 0:
            return "empty_file"

        return "process_comments.replace_nulls"

    check = check_file()

    empty_file = BashOperator(
        task_id="empty_file",
        bash_command="echo 'The input file is empty. Nothing to process.'",
    )

    wait_for_file >> check >> empty_file

    with TaskGroup(group_id="process_comments") as process_comments:

        @task
        def replace_nulls():
            df = pd.read_csv(INPUT_FILE)
            df = df.fillna("-")
            df.to_csv("/opt/airflow/data/step1.csv", index=False)

        @task
        def sort_by_date():
            df = pd.read_csv("/opt/airflow/data/step1.csv")
            df["created_date"] = pd.to_datetime(df["created_date"])
            df = df.sort_values(by="created_date")
            df.to_csv("/opt/airflow/data/step2.csv", index=False)

        @task(outlets=[COMMENTS_ASSET])
        def clean_content():
            df = pd.read_csv("/opt/airflow/data/step2.csv")

            df["content"] = df["content"].apply(
                lambda text: re.sub(
                    r"[^\w\s.,!?;:'\"()\-]",
                    "",
                    str(text),
                )
            )

            df.to_csv(OUTPUT_FILE, index=False)

        replace_nulls() >> sort_by_date() >> clean_content()

    check >> process_comments

comments_pipeline()