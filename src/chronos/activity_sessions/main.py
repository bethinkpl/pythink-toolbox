from datetime import datetime
import logging
from typing import List

from tqdm import tqdm

from chronos.activity_sessions import storage_operations, activity_events_source
from chronos import custom_types
from chronos.storage.schemas import UsersGenerationStatuesSchema

logger = logging.getLogger(__name__)


def main(time_range: custom_types.TimeRange) -> None:
    """Create activity_sessions for all the users
    who had activity_events in a given time range
    and update the materialized views."""

    logger.info(
        "Generating activity sessions for range between %s & %s",
        time_range.start,
        time_range.end,
    )

    generation_start_time = datetime.now()

    generation_id = storage_operations.insert_new_generation(
        time_range=time_range, start_time=generation_start_time
    )

    users_with_failed_last_generation = (
        storage_operations.extract_users_with_failed_last_generation()
    )

    user_ids_to_with_failed_generation = [
        doc["user_id"] for doc in users_with_failed_last_generation
    ]

    _generate_activity_sessions(
        time_range=time_range,
        user_ids_to_with_failed_generation=user_ids_to_with_failed_generation,
    )

    _generate_activity_sessions_for_users_with_failed_status(
        time_range_end=time_range.end,
        users_with_failed_last_generation=users_with_failed_last_generation,
    )

    generation_end_time = datetime.now()
    storage_operations.update_generation_end_time(
        generation_id=generation_id, end_time=generation_end_time
    )
    logger.info("Generation took %s", generation_end_time - generation_start_time)

    storage_operations.update_materialized_views(reference_time=time_range.start)
    logger.info("Materialized views updated 🙌🏼")


def _generate_activity_sessions(
    time_range: custom_types.TimeRange, user_ids_to_with_failed_generation: List[int]
) -> None:

    users_activity_events = (
        activity_events_source.read_activity_events_between_datetimes(
            start_time=time_range.start,
            end_time=time_range.end,
            user_ids=user_ids_to_with_failed_generation,
            user_exclude=True,
        )
    )

    users_activity_events_groups = users_activity_events.groupby("user_id").client_time

    for user_id, activity_events in tqdm(users_activity_events_groups):
        storage_operations.save_new_activity_sessions(
            user_id, activity_events, time_range_end=time_range.end
        )


def _generate_activity_sessions_for_users_with_failed_status(
    time_range_end: datetime,
    users_with_failed_last_generation: List[UsersGenerationStatuesSchema],
) -> None:

    for doc in users_with_failed_last_generation:

        user_id = doc["user_id"]

        activity_events = activity_events_source.read_activity_events_between_datetimes(
            start_time=doc["time_until_generations_successful"],
            end_time=time_range_end,
            user_ids=[user_id],
        ).client_time

        storage_operations.save_new_activity_sessions(
            user_id, activity_events, time_range_end=time_range_end
        )
