from datetime import datetime
import logging

from tqdm import tqdm

from chronos.activity_sessions import storage_operations, activity_events_source

logger = logging.getLogger(__name__)


def main(time_range: storage_operations.TimeRange) -> None:
    """Create activity_sessions for all the users
    who had activity_events in a given time range
    and update the materialized views."""

    logger.info(
        "Generating activity sessions from range between %s & %s",
        time_range.start,
        time_range.end,
    )

    generation_start_time = datetime.now()

    generation_id = storage_operations.insert_new_generation(
        time_range=time_range, start_time=generation_start_time
    )

    user_ids_to_be_excluded_in_activity_events_extraction = (
        storage_operations.extract_users_in_user_generation_failed_collection()
    )

    users_activity_events = (
        activity_events_source.read_activity_events_between_datetimes(
            start_time=time_range.start,
            end_time=time_range.end,
            exclude_user_ids=user_ids_to_be_excluded_in_activity_events_extraction,
        )
    )

    logger.info(
        "Creating activity sessions from %i activity_events", len(users_activity_events)
    )

    users_activity_events_groups = users_activity_events.groupby("user_id").client_time

    for user_id, activity_events in tqdm(users_activity_events_groups):

        storage_operations.save_new_activity_sessions(
            user_id, activity_events, reference_time=time_range.start
        )

    storage_operations.update_materialized_views(reference_time=time_range.start)

    generation_end_time = datetime.now()
    storage_operations.update_generation_end_time(
        generation_id=generation_id, end_time=generation_end_time
    )

    logger.info("Generation took %s", generation_end_time - generation_start_time)
