import logging
from typing import Optional, Dict, Union
from datetime import datetime

import bson
import pandas as pd
import pymongo
from pymongo.client_session import ClientSession
import pymongo.errors

import chronos.activity_sessions
from chronos.storage import get_client, get_activity_sessions_collection

logger = logging.getLogger(__name__)


class MongoCommitError(Exception):
    """Error that occurred while performing MongoDB commit."""


def main(user_id: int, activity_events: pd.Series, reference_time: datetime) -> None:
    """Perform all operations to create user activity_sessions & save it to storage."""

    logger.info("Run test_activity_sessions mongo operations for user_id %i", user_id)

    with get_client().start_session() as session:
        try:
            _run_user_crud_operations_transaction(
                user_id=user_id,
                activity_events=activity_events,
                session=session,
            )
        except MongoCommitError:
            pass  # TODO LACE-471

        _run_materialized_views_update(session=session, reference_time=reference_time)


def _run_user_crud_operations_transaction(
    user_id: int, activity_events: pd.Series, session: ClientSession
) -> None:

    with session.start_transaction(write_concern=pymongo.WriteConcern(w="majority")):
        collection = get_activity_sessions_collection()

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

            raise MongoCommitError(
                "Error during test_activity_sessions creation transaction commit."
            ) from err


def _run_materialized_views_update(
    session: ClientSession, reference_time: datetime  # pylint: disable=unused-argument
) -> None:

    # maybe asynchronous update materialized view after every user?
    #   materialized_views = [view1, view2, view3, view4]
    #   for materialized_view in materialized_views:
    #       update_materialized_view(materialized_view=materialized_view, start_time=start_time)
    pass  # TODO LACE-459
