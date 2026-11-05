
from datetime import datetime
import os
import re

import pandas as pd

from airflow.sdk import dag, task, Asset
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


INPUT_FILE: str | None = os.getenv("INPUT_FILE")
STEP1_FILE: str | None = os.getenv("STEP1_FILE")
STEP2_FILE: str | None = os.getenv("STEP2_FILE")
OUTPUT_FILE: str | None = os.getenv("OUTPUT_FILE")

COMMENTS_ASSET: Asset = Asset("processed_comments")


@dag(
    dag_id="comments_pipeline",
    description="Waiting and updating comments from CSV",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["comments", "pandas"],
)
def comments_pipeline() -> None:
    """
    Process comments from a CSV file.

    Waits for the input file, checks whether it is empty,
    replaces missing values, sorts records by date,
    and cleans comment content.
    """

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    @task.branch
    def check_file() -> str:
        """Check whether the input CSV file is empty and select the next task."""
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
        def replace_nulls() -> None:
            """Replace missing values in the input CSV with hyphens."""
            df: pd.DataFrame = pd.read_csv(INPUT_FILE)
            df = df.fillna("-")
            df.to_csv(STEP1_FILE, index=False)

        @task
        def sort_by_date() -> None:
            """Sort comments by creation date in ascending order."""
            df: pd.DataFrame = pd.read_csv(STEP1_FILE)
            df["created_date"] = pd.to_datetime(df["created_date"])
            df = df.sort_values(by="created_date")
            df.to_csv(STEP2_FILE, index=False)

        @task(outlets=[COMMENTS_ASSET])
        def clean_content() -> None:
            """Remove emojis and unwanted characters from comment content."""
            df: pd.DataFrame = pd.read_csv(STEP2_FILE)

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
