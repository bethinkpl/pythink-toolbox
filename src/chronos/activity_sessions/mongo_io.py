from typing import Optional

import pandas as pd
import pymongo

import chronos.settings

client = pymongo.MongoClient(
    host=chronos.settings.MONGO_HOST,
    port=chronos.settings.MONGO_PORT,
    username=chronos.settings.MONGO_USERNAME,
    password=chronos.settings.MONGO_PASSWORD,
)

database = client.get_database(chronos.settings.MONGO_DATABASE)


def read_last_active_session_for_user(
    user_id: int,  # pylint: disable=unused-argument
) -> Optional[pd.DataFrame]:
    # FIXME move to different place? pylint: disable=fixme
    # FIXME pop last active session from mongo pylint: disable=fixme
    return pd.DataFrame(
        {
            "start_time": [pd.Timestamp("2018-12-14T10:40:19.691Z")],
            "end_time": [pd.Timestamp("2018-12-14T10:45:28.421Z")],
        }
    )
