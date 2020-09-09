import os

from typing import Union

from pathlib import Path
from datetime import datetime

import dotenv
import datatosk
import pandas as pd  # type: ignore[import]


ENV_PATH = Path("../..") / ".env"
dotenv.load_dotenv(dotenv_path=ENV_PATH)

BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")


def read(
    start_time: Union[datetime, str], end_time: Union[datetime, str]
) -> pd.DataFrame:
    """
    Read activity events from bigquery
    """

    bigquery_source = datatosk.gbq("prod")

    query = f"""
        SELECT user_id, DATETIME(client_time) as client_time
        FROM `{BIGQUERY_PLATFORM_DATASET_ID}.user_activity_events`
        WHERE client_time BETWEEN @start_time AND @end_time
        GROUP BY user_id, client_time
        ORDER BY user_id, client_time;
        """

    params = {
        "start_time": start_time,
        "end_time": end_time,
    }

    return bigquery_source.read(query=query, params=params)
