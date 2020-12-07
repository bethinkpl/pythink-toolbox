from datetime import datetime
from typing import Dict, List, Union

import datatosk
import pandas as pd

from chronos.settings import BIGQUERY_PLATFORM_DATASET_ID


def read_activity_events_between_datetimes(
    start_time: datetime,
    end_time: datetime,
    user_ids: List[int],
    user_exclude: bool = False,
) -> pd.DataFrame:
    """Read activity events from bigquery."""

    bigquery_source = datatosk.gbq("prod")

    query = f"""
        SELECT user_id, DATETIME(client_time) as client_time
        FROM `{BIGQUERY_PLATFORM_DATASET_ID}.user_activity_events`
        WHERE client_time >= @start_time
          AND client_time < @end_time
          AND user_id{" NOT " if user_exclude else " "}IN UNNEST(@user_ids)
        GROUP BY user_id, client_time
        ORDER BY user_id, client_time;
        """

    params: Dict[str, Union[datetime, List[int]]] = {
        "start_time": start_time,
        "end_time": end_time,
        "user_ids": user_ids,
    }

    return bigquery_source.read(query=query, params=params)
