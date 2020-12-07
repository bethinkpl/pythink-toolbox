import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterator, List

import pandas as pd
from tqdm import tqdm

from chronos import custom_types, settings
from chronos.activity_sessions import activity_events_source, storage_operations
from chronos.activity_sessions.storage_operations import (
    extract_min_last_successful_generation_end_time,
    read_last_generation_time_range_end,
)
from chronos.storage.schemas import UsersGenerationStatuesSchema

logger = logging.getLogger(__name__)

ACTIVITY_EVENTS_TABLE_CREATION = datetime(2019, 8, 11)


def main() -> None:
    """Activity sessions generation entry point."""

    logger.info("GENERATING ACTIVITY SESSIONS PROCEDURE INITIATED 🤠")

    time_range = custom_types.TimeRange(
        start=read_last_generation_time_range_end() or ACTIVITY_EVENTS_TABLE_CREATION,
        end=datetime.now(),
    )

    earliest_activity_session_change_time = (
        extract_min_last_successful_generation_end_time() or time_range.start
    )  # has to be extracted in the beginning, cuz it can change value during activity sessions generation

    _run_activity_sessions_generation(
        time_range=time_range,
    )

    storage_operations.update_materialized_views(
        reference_time=earliest_activity_session_change_time
    )


def _run_activity_sessions_generation(
    time_range: custom_types.TimeRange,
) -> None:
    """Create activity_sessions for all the users
    who had activity_events in a given time range."""

    logger.info(
        "Generating activity sessions for range between %s & %s",
        time_range.start,
        time_range.end,
    )

    interval_time_ranges = _calculate_intervals_for_time_range(
        time_range=time_range,
        interval_size=settings.ACTIVITY_SESSIONS_GENERATION_INTERVAL_SIZE,
    )
    for interval_time_range in tqdm(interval_time_ranges):

        user_ids_and_time_when_last_status_failed_from_generations = (
            storage_operations.extract_user_ids_and_time_when_last_status_failed_from_generations()
        )

        with _save_generation_data(time_range=interval_time_range):
            _generate_activity_sessions(
                time_range=interval_time_range,
                user_ids_to_exclude=[
                    doc["user_id"]
                    for doc in user_ids_and_time_when_last_status_failed_from_generations
                ],
            )

            _generate_activity_sessions_for_users_with_failed_status(
                time_range_end=interval_time_range.end,
                user_ids_and_start_times=user_ids_and_time_when_last_status_failed_from_generations,
            )


def _calculate_intervals_for_time_range(
    time_range: custom_types.TimeRange,
    interval_size: timedelta,
) -> List[custom_types.TimeRange]:

    intervals_starts = pd.date_range(
        start=time_range.start,
        end=time_range.end,
        freq=interval_size,
    )

    intervals = (
        pd.DataFrame({"start": intervals_starts})
        .assign(end=lambda df: df.start.shift(-1))
        .fillna(time_range.end)
        .query("start != end")
        .to_dict(orient="records")
    )

    return [custom_types.TimeRange(**interval) for interval in intervals]


@contextmanager
def _save_generation_data(time_range: custom_types.TimeRange) -> Iterator[None]:
    start_time = datetime.now()
    generation_id = storage_operations.insert_new_generation(
        time_range=time_range, start_time=start_time
    )
    yield None

    end_time = datetime.now()
    storage_operations.update_generation_end_time(
        generation_id=generation_id, end_time=end_time
    )
    logger.info("Generation took %s", end_time - start_time)


def _generate_activity_sessions(
    time_range: custom_types.TimeRange, user_ids_to_exclude: List[int]
) -> None:

    users_activity_events = (
        activity_events_source.read_activity_events_between_datetimes(
            start_time=time_range.start,
            end_time=time_range.end,
            user_ids=user_ids_to_exclude,
            user_exclude=True,
        )
    )

    users_activity_events_groups = users_activity_events.groupby("user_id").client_time

    for user_id, activity_events in users_activity_events_groups:
        storage_operations.save_new_activity_sessions(
            user_id, activity_events, time_range_end=time_range.end
        )


def _generate_activity_sessions_for_users_with_failed_status(
    time_range_end: datetime,
    user_ids_and_start_times: List[UsersGenerationStatuesSchema],
) -> None:

    for user_id_and_start_time in user_ids_and_start_times:

        user_id: int = user_id_and_start_time["user_id"]
        start_time: datetime = user_id_and_start_time[
            "last_successful_generation_end_time"
        ]

        activity_events = activity_events_source.read_activity_events_between_datetimes(
            start_time=start_time,
            end_time=time_range_end,
            user_ids=[user_id],
        ).client_time

        storage_operations.save_new_activity_sessions(
            user_id=user_id,
            activity_events=activity_events,
            time_range_end=time_range_end,
        )
