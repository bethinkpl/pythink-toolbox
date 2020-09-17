# TODO notes: handle how to constrain dates of new queries to read_activity_events and materialized_views
from datetime import datetime
import logging

from tqdm import tqdm

import chronos.activity_sessions.creation_transformations
import chronos.activity_sessions.storage_operations

logger = logging.getLogger(__name__)


def main(start_time: datetime, end_time: datetime) -> None:
    """Create activity_sessions for all users
    who had activity_events between given timestamps."""

    users_activity_events = chronos.activity_sessions.activity_events_source.read_activity_events_between_datetimes(
        start_time=start_time, end_time=end_time
    )

    logger.info(
        "Creating activity sessions from %i activity_events", len(users_activity_events)
    )

    users_activity_events_groups = users_activity_events.groupby("user_id").client_time

    for user_id, activity_events in tqdm(users_activity_events_groups):
        chronos.activity_sessions.storage_operations.main(
            user_id, activity_events, reference_time=start_time
        )
