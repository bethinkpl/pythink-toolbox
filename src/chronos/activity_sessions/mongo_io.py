import logging
from typing import Optional, Dict, Union
from datetime import datetime

import bson
import pandas as pd
import pymongo
import pymongo.errors

import chronos.activity_sessions
import chronos.settings

logger = logging.getLogger(__name__)

CLIENT = pymongo.MongoClient(
    host=chronos.settings.MONGO_HOST,
    port=chronos.settings.MONGO_PORT,
    username=chronos.settings.MONGO_USERNAME,
    password=chronos.settings.MONGO_PASSWORD,
)

CHRONOS_DB = CLIENT[chronos.settings.MONGO_DATABASE]
ACTIVITY_SESSIONS_COLLECTION: pymongo.collection.Collection = (
    CHRONOS_DB.activity_sessions
)


class MongoCommitError(Exception):
    """Error that occurred while performing MongoDB commit."""


def main(user_id: int, activity_events: pd.Series, start_time: datetime):
    logger.info("Run test_activity_sessions mongo operations for user_id i%", user_id)

    with CLIENT.start_session() as session:
        try:
            _run_create_user_activity_sessions_transaction(
                user_id=user_id,
                activity_events=activity_events,
                session=session,
            )
        except MongoCommitError:
            pass  # TODO LACE-471

        _run_materialized_views_update(session=session, start_time=start_time)


def _run_create_user_activity_sessions_transaction(
    user_id: int, activity_events: pd.Series, session
) -> None:

    with session.start_transaction(write_concern=pymongo.WriteConcern(w="majority")):

        last_active_session: Optional[
            Dict[str, Union[datetime, bson.ObjectId]]
        ] = ACTIVITY_SESSIONS_COLLECTION.find_one_and_delete(
            filter={"user_id": user_id, "is_active": True},
            projection={"_id": 0, "start_time": 1, "end_time": 1},
            sort=[("end_time", pymongo.DESCENDING)],
            session=session,
        )

        logger.debug(
            "last_active_session from mongo: \n{last_active_session}".format(
                last_active_session=last_active_session
            )
        )

        user_activity_sessions = chronos.activity_sessions.create_activity_sessions.create_user_activity_sessions(
            user_id=user_id,
            activity_events=activity_events,
            last_active_session=last_active_session,
        )

        ACTIVITY_SESSIONS_COLLECTION.insert_many(
            user_activity_sessions, session=session
        )

        _commit_transaction_with_retry(session=session)
        logger.info("Transaction committed for user {user_id}.".format(user_id=user_id))


def _commit_transaction_with_retry(session) -> None:
    while True:
        try:
            session.commit_transaction()

            break
        except (
            pymongo.errors.ConnectionFailure,
            pymongo.errors.OperationFailure,
        ) as err:
            # Can retry commit
            if err.has_error_label("UnknownTransactionCommitResult"):
                logging.error(
                    "UnknownTransactionCommitResult, retrying " "commit operation ..."
                )
                continue
            else:
                raise MongoCommitError(
                    "Error during test_activity_sessions creation transaction commit."
                ) from err


def _run_materialized_views_update(session, start_time: datetime):

    # maybe asynchronous update materialized view after every user?
    #   materialized_views = [view1, view2, view3, view4]
    #   for materialized_view in materialized_views:
    #       update_materialized_view(materialized_view=materialized_view, start_time=start_time)
    pass  # TODO LACE-459
