from pathlib import Path
from typing import List
import logging as log

import datatosk
import dotenv
import pandas as pd

ENV_PATH = Path(".") / ".env"
dotenv.load_dotenv(dotenv_path=ENV_PATH)

mongo_source = datatosk.mongodb("chronos")


def read_learning_time(user_ids: List) -> List:
    return mongo_source.read.to_list(
        collection="learning_time_daily_view",
        result_filter={"user_id": {"$in": user_ids}},
    )


def write_activity_sessions(activity_sessions: pd.DataFrame) -> None:
    mongo_source.write(collection="activity_sessions", data=activity_sessions)
    log.info(f"Wrote {len(activity_sessions)} activity sessions to storage.")


def read_activity_sessions_by_user(user_id: int) -> pd.DataFrame:
    return mongo_source.read.to_pandas(
        collection="activity_sessions", result_filter={"user_id": user_id},
    )


if __name__ == "__main__":
    # FIXME Debug code, remove in production version.
    # Learning time reading example
    print(f"read_learning_time: {read_learning_time([1])}")

    # activity sessions writing example
    write_activity_sessions(
        pd.DataFrame(
            columns=[
                "user_id",
                "session_start",
                "session_end",
                "is_active",
                "is_break",
                "is_focus",
            ],
            data=[
                [
                    123,
                    pd.Timestamp("2018-12-14T11:00:54.860Z"),
                    pd.Timestamp("2018-12-14T11:06:08.533Z"),
                    True,
                    False,
                    False,
                ],
                [
                    123,
                    pd.Timestamp("2018-12-14T11:06:08.533Z"),
                    pd.Timestamp("2018-12-14T11:10:07.644Z"),
                    False,
                    False,
                    False,
                ],
            ],
        )
    )

    # Retrieving sessions by user ID
    print(
        f"read_activity_sessions_by_user:\n{read_activity_sessions_by_user(user_id=123)}"
    )
