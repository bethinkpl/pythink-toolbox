import logging
from typing import Optional, Dict, Union
from datetime import datetime

import bson
import pandas as pd
import pymongo
import pymongo.errors
from pymongo.client_session import ClientSession
from pymongo.collection import Collection

import chronos.activity_sessions
from chronos.storage import get_client, get_activity_sessions_collection

logger = logging.getLogger(__name__)


class _MongoCommitError(Exception):
    """Error that occurred while performing MongoDB commit."""


def main(user_id: int, activity_events: pd.Series, reference_time: datetime) -> None:
    """Perform all operation to create user activity_sessions & save it to storage."""

    logger.info("Run test_activity_sessions mongo operations for user_id %i", user_id)

    collection = get_activity_sessions_collection()

    with get_client().start_session() as session:
        try:
            _run_user_crud_operations_transaction(
                user_id=user_id,
                activity_events=activity_events,
                session=session,
                collection=collection,
            )
        except _MongoCommitError:
            raise  # TODO LACE-471

        _run_materialized_views_update(
            reference_time=reference_time, collection=collection
        )


def _run_user_crud_operations_transaction(
    user_id: int,
    activity_events: pd.Series,
    session: ClientSession,
    collection: Collection,
) -> None:

    with session.start_transaction(write_concern=pymongo.WriteConcern(w="majority")):

        last_active_session: Optional[
            Dict[str, Union[datetime, bson.ObjectId]]
        ] = collection.find_one_and_delete(
            filter={"user_id": user_id, "is_active": True},
            projection={"_id": 0, "start_time": 1, "end_time": 1},
            sort=[("end_time", pymongo.DESCENDING)],
            session=session,
        )

        logger.debug("last_active_session from mongo: \n%s", last_active_session)

        user_activity_sessions = chronos.activity_sessions.creation_transformations.generate_user_activity_sessions(
            user_id=user_id,
            activity_events=activity_events,
            last_active_session=last_active_session,
        )

        collection.insert_many(user_activity_sessions, session=session)

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
            # Can retry commit
            if err.has_error_label("UnknownTransactionCommitResult"):
                logging.error(
                    "UnknownTransactionCommitResult, retrying " "commit operation ..."
                )
                continue

            raise _MongoCommitError(
                "Error during test_activity_sessions creation transaction commit."
            ) from err


def _run_materialized_views_update(
    reference_time: datetime, collection: Collection
) -> None:

    # maybe asynchronous update materialized view after every user?
    #   materialized_views = [view1, view2, view3, view4]
    #   for materialized_view in materialized_views:
    #       update_materialized_view(materialized_view=materialized_view, start_time=start_time)

    match_step = {
        "$match": {
            "$or": [{"is_active": {"$eq": True}}, {"is_break": {"$eq": True}}],
            "start_time": {"$gte": reference_time},
        }
    }

    group_step = {
        "$group": {
            "_id": {
                "user_id": "$user_id",
                "date_hour": {
                    "$dateToString": {"format": "%Y-%m-%d %H", "date": "$start_time"}
                },  # FIXME cannot just cast start_time to string - this leads to more than one hour aggregated times
            },
            "duration_ms": {"$sum": {"$subtract": ["$end_time", "$start_time"]}},
        }
    }

    project_step = {
        "$project": {
            "_id": bson.ObjectId(),
            "user_id": "$_id.user_id",
            "time_range": {
                "start": {
                    "$dateFromString": {
                        "dateString": "$_id.date_hour",
                        "format": "%Y-%m-%d %H",
                    }
                },
                "end": {
                    "$add": [
                        {
                            "$dateFromString": {
                                "dateString": "$_id.date_hour",
                                "format": "%Y-%m-%d %H",
                            }
                        },
                        60 * 60 * 1000,
                    ]
                },
            },
            "duration_ms": 1,
        }
    }
    merge_step = {
        "$merge": {"into": "daily_learning_time_mv", "whenMatched": "replace"}
    }

    collection.aggregate([match_step, group_step, project_step, merge_step])


if __name__ == "__main__":
    _run_materialized_views_update(
        reference_time=datetime(1970, 1, 1),
        collection=get_activity_sessions_collection(),
    )
