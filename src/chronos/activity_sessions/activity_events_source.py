from typing import Union, Optional, List, Dict
from datetime import datetime

import datatosk
import pandas as pd

from chronos.settings import BIGQUERY_PLATFORM_DATASET_ID


def read_activity_events_between_datetimes(
    start_time: Union[datetime, str],
    end_time: Union[datetime, str],
    exclude_user_ids: Optional[List[int]] = None,
) -> pd.DataFrame:
    """
    Read activity events from bigquery
    """

    bigquery_source = datatosk.gbq("prod")

    params: Dict[str, Union[datetime, str, List[int]]] = {
        "start_time": start_time,
        "end_time": end_time,
    }

    exclude_user_ids_condition = ""
    if exclude_user_ids:
        exclude_user_ids_condition = "AND user_id NOT IN @exclude_user_ids"
        params["exclude_user_ids"] = exclude_user_ids

    query = f"""
        SELECT user_id, DATETIME(client_time) as client_time
        FROM `{BIGQUERY_PLATFORM_DATASET_ID}.user_activity_events`
        WHERE client_time >= @start_time
          AND client_time < @end_time
          {exclude_user_ids_condition}
        GROUP BY user_id, client_time
        ORDER BY user_id, client_time;
        """

    return bigquery_source.read(query=query, params=params)
