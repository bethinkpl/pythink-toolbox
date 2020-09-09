# TODO
# 1. read activity_events
# 2. for each user in activity_events:
# - initialize_transaction
# - read last_active_sessions
# - delete last_active_sessions
# - create activity_sessions
# - insert activity_sessions
# 3. run function of materialized views

# notes: handle how to constrain dates of new queries to read_activity_events and materialized_views
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

        with chronos.activity_sessions.mongo_io.client.start_session() as session:
            session.with_transaction(
                # TODO
            )

            def callback(session):
                last_active_session = chronos.activity_sessions.mongo_io.read_last_active_session_for_user(
                    user_id
                )
                # TODO delete this session

                user_activity_sessions = chronos.activity_sessions.create_activity_sessions.create_user_activity_sessions(
                    user_id=user_id,
                    activity_events=activity_events,
                    last_active_session=last_active_session,
                )

                # TODO insert user_activity_sessions

            # TODO end of transaction scope

    # TODO run materialized_views functions
    #   materialized_views = [view1, view2, view3, view4]
    #   for materialized_view in materialized_views:
    #       update_materialized_view(materialized_view=materialized_view, start_time=start_time)


if __name__ == "__main__":
    # FIXME move to cli.py
    start_time_ = datetime(2000, 1, 1)
    end_time_ = datetime(2000, 1, 1)

    main(start_time=start_time_, end_time=end_time_)
