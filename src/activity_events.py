# from os import getenv
from typing import Union

from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
import datatosk
import pandas as pd


ENV_PATH = Path("..") / ".env"  # FIXME .. to .
load_dotenv(dotenv_path=ENV_PATH)


# BIGQUERY_PLATFORM_DATASET_ID: str = getenv("BIGQUERY_DATASET_ID_WNL_PLATFORM", None)
BIGQUERY_PLATFORM_DATASET_ID = "lek_platform"


def read(
    start_date: Union[datetime, str], end_date: Union[datetime, str]
) -> pd.DataFrame:

    bigquery_source = datatosk.gbq("prod")

    query = f"""
        SELECT user_id, client_time
        FROM `{BIGQUERY_PLATFORM_DATASET_ID}.user_activity_events`
        WHERE client_time BETWEEN @start_date AND @end_date 
        ORDER BY user_id, client_time ASC;
        """

    params = {
        "start_date": start_date,
        "end_date": end_date,
    }

    return bigquery_source.read(query=query, params=params).drop_duplicates()


if __name__ == "__main__":

    start = datetime.now()
    pyk = read("2020-08-24", datetime.now())
    end = datetime.now()
    print(pyk)
    print((end - start).total_seconds())
