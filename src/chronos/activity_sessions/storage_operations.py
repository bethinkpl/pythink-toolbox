import logging
from typing import Optional, Dict, Union, List
from datetime import datetime

import bson
import pandas as pd
import pymongo
import pymongo.errors
from pymongo.client_session import ClientSession

import chronos.activity_sessions.generation_operations
from chronos.storage.specs import mongodb
from chronos.storage.schemas import UserGenerationFailedSchema

logger = logging.getLogger(__name__)


def save_new_activity_sessions(
    user_id: int, activity_events: pd.Series, reference_time: datetime
) -> None:
    """Perform all operation to create user activity_sessions & save it to storage."""

    logger.info("Run test_activity_sessions mongo operations for user_id %i", user_id)

    with mongodb.client.start_session() as session:

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

            mongodb.collections.user_generation_failed.insert_one(
                UserGenerationFailedSchema(
                    user_id=user_id, reference_time=reference_time
                )
            )


def update_materialized_views(reference_time: datetime) -> None:
    """Updates all materialized views.
    Args:
        reference_time: time from which materialized views takes data to update themselves.
    """

    for materialized_view in [
        mongodb.materialized_views.learning_time_sessions_duration,
        mongodb.materialized_views.break_sessions_duration,
        mongodb.materialized_views.focus_sessions_duration,
    ]:
        materialized_view.run_aggregation(reference_time=reference_time)


def extract_users_in_user_generation_failed_collection() -> List[int]:
    """Extracts all user_id in generation_failed collection documents.
    Returns:
        List of user_ids
    """
    return [
        doc["user_id"] for doc in mongodb.collections.user_generation_failed.find({})
    ]


def _run_user_crud_operations_transaction(
    user_id: int, activity_events: pd.Series, session: ClientSession
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
