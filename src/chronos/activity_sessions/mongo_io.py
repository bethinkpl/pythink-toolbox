import logging
from typing import Optional, Dict, Union
from datetime import datetime

import bson
import pandas as pd
import pymongo
import pymongo.errors

import chronos.activity_sessions
import chronos.settings

# CLIENT = pymongo.MongoClient(
#     host=chronos.settings.MONGO_HOST,
#     port=chronos.settings.MONGO_PORT,
#     username=chronos.settings.MONGO_USERNAME,
#     password=chronos.settings.MONGO_PASSWORD,
# )
CLIENT = pymongo.MongoClient(
    "mongodb://root:local@mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=replica_set0"
)

ACTIVITY_SESSIONS_COLLECTION: pymongo.collection.Collection = CLIENT.get_database(
    chronos.settings.MONGO_DATABASE
).activity_sessions


class MongoCommitError(Exception):
    """FIXME"""


def main(user_id: int, activity_events: pd.Series):

    with CLIENT.start_session() as session:
        try:
            _run_create_sessions_transaction(
                user_id=user_id,
                activity_events=activity_events,
                session=session,
            )
        except MongoCommitError:
            pass  # FIXME

        # TODO run materialized_views functions (maybe asynchronous update materialized view after every user?
        #   materialized_views = [view1, view2, view3, view4]
        #   for materialized_view in materialized_views:
        #       update_materialized_view(materialized_view=materialized_view, start_time=start_time)


def _run_create_sessions_transaction(
    user_id: int, activity_events: pd.Series, session
) -> None:  # FIXME rename

    with session.start_transaction(write_concern=pymongo.WriteConcern(w="majority")):

        last_active_session: Optional[
            Dict[str, Union[datetime, bson.ObjectId]]
        ] = ACTIVITY_SESSIONS_COLLECTION.find_one_and_delete(
            filter={"user_id": user_id, "is_focus": True},
            projection={"_id": 0, "start_time": 1, "end_time": 1},
            sort=[("end_time", pymongo.DESCENDING)],
            session=session,
        )

        print(f"{last_active_session=}")  # FIXME to logging

        user_activity_sessions = chronos.activity_sessions.create_activity_sessions.create_user_activity_sessions(
            user_id=user_id,
            activity_events=activity_events,
            last_active_session=last_active_session,
        )

        print(f"{user_activity_sessions=}")  # FIXME to logging

        ACTIVITY_SESSIONS_COLLECTION.insert_many(
            user_activity_sessions, session=session
        )

        _commit_with_retry(session=session)  # FIXME rename


def _commit_with_retry(session) -> None:
    while True:
        try:
            session.commit_transaction()
            print("Transaction committed.")
            break
        except (
            pymongo.errors.ConnectionFailure,
            pymongo.errors.OperationFailure,
        ) as exc:
            # Can retry commit
            if exc.has_error_label("UnknownTransactionCommitResult"):
                logging.error(
                    "UnknownTransactionCommitResult, retrying " "commit operation ..."
                )
                continue
            else:
                logging.warning("Error during commit ...")
                raise MongoCommitError


# FIXME add logging


if __name__ == "__main__":
    import datetime

    main(11, pd.Series([datetime.datetime(2000, 1, 1)]))
