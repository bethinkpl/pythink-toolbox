import logging
from typing import Optional, Dict, Union, List
from datetime import datetime

import bson
import pandas as pd
import pymongo
import pymongo.errors
from pymongo.client_session import ClientSession

import chronos.activity_sessions
from chronos.storage import mongodb, MongoCommitError

logger = logging.getLogger(__name__)


def save_new_activity_sessions(
    user_id: int, activity_events: pd.Series, reference_time: datetime
) -> None:
    """Perform all operation to create user activity_sessions & save it to storage."""

    logger.info("Run test_activity_sessions mongo operations for user_id %i", user_id)

    with mongodb.client.start_session() as session:

        try:
            _run_user_crud_operations_transaction(
                user_id=user_id,
                activity_events=activity_events,
                session=session,
            )
        except Exception as err:
            logger.error(
                f"{_run_user_crud_operations_transaction.__name__} has failed with error:\n{err}"
            )

            mongodb.collections.user_generation_failed.insert_one(
                {"user_id": user_id, "reference_time": reference_time}
            )


def update_materialized_views(reference_time: datetime) -> None:

    for materialized_view in mongodb.materialized_views:
        materialized_view.update(reference_time=reference_time)


def extract_users_in_user_generation_failed_collection() -> List[int]:
    return [
        doc["user_id"]
        for doc in chronos.storage.mongodb.collections.user_generation_failed.find({})
    ]


def _run_user_crud_operations_transaction(
    user_id: int,
    activity_events: pd.Series,
    session: ClientSession,
) -> None:

    with session.start_transaction(
        write_concern=pymongo.WriteConcern(w="majority")
    ):  # TODO handle read_, write_concerns, and read_preference

        collection = mongodb.collections.activity_sessions

        last_active_session: Optional[
            Dict[str, Union[datetime, bson.ObjectId]]
        ] = collection.find_one_and_delete(
            filter={"user_id": user_id, "is_active": True},
            projection={"_id": 0, "start_time": 1, "end_time": 1},
            sort=[("end_time", pymongo.DESCENDING)],
            session=session,
        )

        logger.debug("last_active_session from mongo: \n%s", last_active_session)

        user_activity_sessions = chronos.activity_sessions.generation_operations.generate_user_activity_sessions(
            user_id=user_id,
            activity_events=activity_events,
            last_active_session=last_active_session,
        )

        collection.insert_many(
            user_activity_sessions, session=session
        )  # TODO LACE-487 add schema version
        # TODO LACE-488 add schema validation
        # TODO consider document per user_id with "sessions_array" or "date_array" - the Bucket Pattern

        _commit_transaction_with_retry(session=session)
        logger.info("Transaction committed for user %i.", user_id)


def _commit_transaction_with_retry(session: ClientSession) -> None:
    while True:
        try:
            session.commit_transaction()
            break
        except (
            pymongo.errors.ConnectionFailure,
            pymongo.errors.OperationFailure,
        ) as err:
            if err.has_error_label("UnknownTransactionCommitResult"):
                logging.error(
                    "UnknownTransactionCommitResult, retrying " "commit operation ..."
                )
                continue

            raise MongoCommitError(
                "Error during test_activity_sessions creation transaction commit."
            ) from err


# TODO example query for daily_learning_time:
#  [
#  {
#      $match:
#          {
#              "_id.user_id": 2,
#              "_id.end_time": {$gt: ISODate("2019-01-01")}}
#  },
#  {
#     $group:
#         {
#             _id: {
#                 "user_id": "$_id.user_id",
#                 "date": {
#                     "$dateToString": {"format": "%Y-%m-%d", "date": "$_id.start_time"}
#                 }
#             },
#             duration_ms: {$sum: "$duration_ms"}
#         }
#   },
#  {
#      $project: {
#          _id: 0,
#          user_id: "$_id.user_id",
#          date: "$_id.date",
#          duration_ms: 1
#      }
#  },
#  {
#      $sort: {date: 1}
#  }
#      ]
