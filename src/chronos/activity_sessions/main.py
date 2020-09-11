# TODO notes: handle how to constrain dates of new queries to read_activity_events and materialized_views
from datetime import datetime

from tqdm import tqdm

import chronos.activity_sessions.create_activity_sessions
import chronos.activity_sessions.mongo_io


def main(start_time: datetime, end_time: datetime) -> None:
    """Create activity_sessions for all users
    who had activity_events between given timestamps."""

    users_activity_events = chronos.activity_sessions.activity_events.read(
        start_time=start_time, end_time=end_time
    )

    users_activity_events_groups = users_activity_events.groupby("user_id").client_time

    for user_id, activity_events in tqdm(users_activity_events_groups):
        chronos.activity_sessions.mongo_io.main(user_id, activity_events)


if __name__ == "__main__":
    # FIXME move to cli.py
    start_time_ = datetime(2000, 1, 1)
    end_time_ = datetime(2000, 1, 1)

    main(start_time=start_time_, end_time=end_time_)
