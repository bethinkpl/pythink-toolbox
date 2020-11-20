import logging
from typing import Optional, Dict, Union, List, Literal
from datetime import datetime

import bson
import pandas as pd
import pymongo
import pymongo.errors
from pymongo.client_session import ClientSession

import chronos
from chronos.activity_sessions.generation_operations import (
    generate_user_activity_sessions,
)
from chronos.custom_types import TimeRange
from chronos.storage.mongo_specs import mongo_specs
from chronos.storage.schemas import UsersGenerationStatuesSchema, GenerationsSchema

logger = logging.getLogger(__name__)


def save_new_activity_sessions(
    user_id: int, activity_events: pd.Series, time_range_end: datetime
) -> None:
    """Perform all operations to create user activity_sessions & save it to storage."""

    if activity_events.empty:
        return

    logger.info(
        "Run save_new_activity_sessions mongo operations for user_id %i", user_id
    )

    with mongo_specs.client.start_session() as session:
        status: Literal["failed", "succeed"]
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
            status = "failed"

        else:
            status = "succeed"

        finally:
            _users_generation_statuses_update(
                user_id=user_id, status=status, time_range_end=time_range_end
            )


def _users_generation_statuses_update(
    user_id: int, status: Literal["failed", "succeed"], time_range_end: datetime
) -> None:
    generation_status = UsersGenerationStatuesSchema(
        user_id=user_id, version=chronos.__version__
    )

    generation_status["last_status"] = status

    if status == "succeed":
        generation_status["time_until_generations_successful"] = time_range_end

    mongo_specs.collections.users_generation_statuses.update_one(
        filter={"user_id": user_id},
        update={"$set": generation_status},
        upsert=True,
    )


def _run_user_crud_operations_transaction(
    user_id: int, activity_events: pd.Series, session: ClientSession
) -> None:

    with session.start_transaction(write_concern=pymongo.WriteConcern(w="majority")):

        collection = mongo_specs.collections.activity_sessions

        last_active_session: Optional[
            Dict[str, Union[datetime, bson.ObjectId]]
        ] = collection.find_one_and_delete(
            filter={"user_id": user_id, "is_active": True},
            projection={"_id": 0, "start_time": 1, "end_time": 1},
            sort=[("end_time", pymongo.DESCENDING)],
            session=session,
        )

        logger.debug("last_active_session from mongo: \n%s", last_active_session)

        user_activity_sessions = generate_user_activity_sessions(
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
    """
    https://docs.mongodb.com/manual/core/transactions-in-applications/#core-api
    """

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

    for materialized_view in mongo_specs.materialized_views.to_list():
        materialized_view.run_aggregation(
            collection=mongo_specs.collections.activity_sessions,
            reference_time=reference_time,
        )


def extract_users_with_failed_last_generation() -> List[UsersGenerationStatuesSchema]:
    """
    Returns:
        List of user_ids
    """

    return list(
        mongo_specs.collections.users_generation_statuses.find(
            filter=UsersGenerationStatuesSchema(last_status="failed"),
            projection={
                "_id": False,
                "user_id": True,
                "time_until_generations_successful": True,
            },
        )
    )


def insert_new_generation(time_range: TimeRange, start_time: datetime) -> bson.ObjectId:
    """Insert new document to `generations` collection.

    Args:
        time_range: time range which generation takes into account
        start_time: start time of generation procedure

    Returns:
        ID of inserted document
    """

    document: GenerationsSchema = {
        "time_range": {"start": time_range.start, "end": time_range.end},
        "start_time": start_time,
        "version": chronos.__version__,
    }

    result = mongo_specs.collections.generations.insert_one(document=document)

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

    mongo_specs.collections.generations.update_one(
        filter={"_id": generation_id}, update={"$set": {"end_time": end_time}}
    )


def read_last_generation_time_range_end() -> datetime:
    """
    Returns:
        Document from generation collection
    with newest `time_range.end` time.
    Raises:
        ValueError when no document with `time_range.end` field
    """

    document = mongo_specs.collections.generations.find_one(
        projection={"_id": False, "time_range.end": True},
        sort=[("time_range.end", pymongo.DESCENDING)],
    )

    if not document:
        raise ValueError("No time_range.end in the collection.")

    time_range_end: datetime = document["time_range"]["end"]
    return time_range_end
