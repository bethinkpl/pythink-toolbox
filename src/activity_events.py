import os
from datetime import datetime
from pathlib import Path
from typing import Union

import datatosk
import dotenv
import pandas as pd  # type: ignore[import]

ENV_PATH = Path(".") / ".env"
dotenv.load_dotenv(dotenv_path=ENV_PATH)

BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")


def read(
    start_date: Union[datetime, str], end_date: Union[datetime, str]
) -> pd.DataFrame:
    """
    Read activity events from bigquery
    """

    bigquery_source = datatosk.gbq("prod")

    query = f"""
        SELECT user_id, DATETIME(client_time) as client_time
        FROM `{BIGQUERY_PLATFORM_DATASET_ID}.user_activity_events`
        WHERE client_time BETWEEN @start_date AND @end_date
        GROUP BY user_id, client_time
        ORDER BY user_id, client_time;
        """

    params = {
        "start_date": start_date,
        "end_date": end_date,
    }

    return bigquery_source.read(query=query, params=params)


if __name__ == "__main__":
    # pylint: disable=fixme
    # FIXME left for debugging, delete before releasing
    start = datetime.now()
    pyk = read("2020-08-24 14:00:00", "2020-08-24 15:00:00")
    end = datetime.now()
    print(pyk)
    print(pyk.info())
    print((end - start).total_seconds())
