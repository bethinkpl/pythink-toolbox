# TODO notes: handle how to constrain dates of new queries to read_activity_events and materialized_views
from datetime import datetime
import logging

from tqdm import tqdm

import chronos.activity_sessions.storage_operations

logger = logging.getLogger(__name__)


def main(start_time: datetime, end_time: datetime) -> None:
    """Create activity_sessions for all users
    who had activity_events between given timestamps."""

    user_ids_to_be_excluded_in_activity_events_extraction = (
        chronos.activity_sessions.storage_operations.extract_users_in_user_generation_failed_collection()
    )

    users_activity_events = chronos.activity_sessions.activity_events_source.read_activity_events_between_datetimes(
        start_time=start_time,
        end_time=end_time,
        exclude_user_ids=user_ids_to_be_excluded_in_activity_events_extraction,
    )

    logger.info(
        "Creating activity sessions from %i activity_events", len(users_activity_events)
    )

    users_activity_events_groups = users_activity_events.groupby("user_id").client_time

    for user_id, activity_events in tqdm(users_activity_events_groups):

        chronos.activity_sessions.storage_operations.save_new_activity_sessions(
            user_id, activity_events, reference_time=start_time
        )

        chronos.activity_sessions.storage_operations.update_materialized_views(
            reference_time=start_time
        )
