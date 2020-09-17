from typing import Union
from datetime import datetime

import datatosk
import pandas as pd

from chronos.settings import BIGQUERY_PLATFORM_DATASET_ID


def read_activity_events_between_datetimes(
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
