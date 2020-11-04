import logging
from typing import Optional, Dict, Union, List, Any
from datetime import datetime

import bson
import pandas as pd
import pymongo
import pymongo.errors
from pymongo.client_session import ClientSession

from chronos.activity_sessions import generation_operations
from chronos.custom_types import TimeRange
from chronos.storage import mongo_specs
from chronos.storage.schemas import UserGenerationFailedSchema, GenerationsSchema

logger = logging.getLogger(__name__)


def save_new_activity_sessions(
    user_id: int, activity_events: pd.Series, reference_time: datetime
) -> None:
    """Perform all operations to create user activity_sessions & save it to storage."""

    logger.info(
        "Run save_new_activity_sessions mongo operations for user_id %i", user_id
    )

    with mongo_specs.client.start_session() as session:

        try:
            _run_user_crud_operations_transaction(
                user_id=user_id, activity_events=activity_events, session=session
            )
        except Exception as err:  # pylint: disable=broad-except
            logger.error(
                "%s has failed with error:\n%s",
                _run_user_crud_operations_transaction.__name__,
                err,
            )

            mongo_specs.collections["user_generation_failed"].insert_one(
                UserGenerationFailedSchema(
                    user_id=user_id, reference_time=reference_time
                )
            )


def _run_user_crud_operations_transaction(
    user_id: int, activity_events: pd.Series, session: ClientSession
) -> None:

    with session.start_transaction(write_concern=pymongo.WriteConcern(w="majority")):

        collection = mongo_specs.collections["activity_sessions"]

        last_active_session: Optional[
            Dict[str, Union[datetime, bson.ObjectId]]
        ] = collection.find_one_and_delete(
            filter={"user_id": user_id, "is_active": True},
            projection={"_id": 0, "start_time": 1, "end_time": 1},
            sort=[("end_time", pymongo.DESCENDING)],
            session=session,
        )

        logger.debug("last_active_session from mongo: \n%s", last_active_session)

        user_activity_sessions = generation_operations.generate_user_activity_sessions(
            user_id=user_id,
            activity_events=activity_events,
            last_active_session=last_active_session,
        )

        collection.insert_many(user_activity_sessions, session=session)

        _commit_transaction_with_retry(session=session)
        logger.info("Transaction committed for user %i.", user_id)


def _commit_transaction_with_retry(
    session: ClientSession,
) -> None:  # TODO test this function
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

            raise RuntimeError(
                "Error during test_activity_sessions creation transaction commit."
            ) from err


def update_materialized_views(reference_time: datetime) -> None:
    """Updates all materialized views.
    Args:
        reference_time: time from which materialized views takes data to update themselves.
    """

    for materialized_view in mongo_specs.materialized_views.values():
        materialized_view.run_aggregation(
            collection=mongo_specs.collections["activity_sessions"],
            reference_time=reference_time,
        )


def extract_users_in_user_generation_failed_collection() -> List[int]:
    """Extracts all user_id in generation_failed collection documents.
    Returns:
        List of user_ids
    """

    return [
        doc["user_id"]
        for doc in mongo_specs.collections["user_generation_failed"].find({})
    ]


def insert_new_generation(time_range: TimeRange, start_time: datetime) -> bson.ObjectId:
    """Insert new document to `generation` collection.

    Args:
        time_range: time range which generation takes into account
        start_time: start time of generation procedure

    Returns:
        ID of inserted document
    """

    document: GenerationsSchema = {
        "time_range": {"start": time_range.start, "end": time_range.end},
        "start_time": start_time,
    }

    result = mongo_specs.collections["generations"].insert_one(document=document)

    return result.inserted_id


def update_generation_end_time(
    generation_id: bson.ObjectId, end_time: datetime
) -> None:
    """Updates `end_time` field in specified document
     in `generations` collection.

    Args:
        generation_id: to specify document
        end_time: value of update
    """

    mongo_specs.collections["generations"].update_one(
        filter={"_id": generation_id}, update={"$set": {"end_time": end_time}}
    )


def read_last_generation_time_range_end() -> Any:
    """
    Returns:
        Document from generation collection
    with newest `time_range.end` time.
    """

    time_range_end = mongo_specs.collections["generations"].find_one(
        projection={"_id": False, "time_range.end": True},
        sort=[("time_range.end", pymongo.DESCENDING)],
    )

    if not time_range_end:
        return None

    return time_range_end["time_range"]["end"]
