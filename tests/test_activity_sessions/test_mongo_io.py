from datetime import datetime

from pytest_steps import test_steps
import pandas as pd

import chronos.activity_sessions.mongo_io as tested_module


def step_clear_activity_sessions_collection():
    tested_module.ACTIVITY_SESSIONS_COLLECTION.delete_many({})


def step_run_main():
    user_id = 500
    activity_events = pd.Series([datetime(2000, 1, 1), datetime(2000, 1, 2)])
    start_time = datetime(2000, 1, 1)
    tested_module.main(
        user_id=user_id, activity_events=activity_events, start_time=start_time
    )


def step_check_data():
    collection = tested_module.ACTIVITY_SESSIONS_COLLECTION.find()

    data_in_collection = []
    for document in collection:
        data_in_collection.append(
            {key: value for key, value in document.items() if key != "_id"}
        )

    expected_data = [
        {
            "user_id": 500,
            "start_time": datetime(1999, 12, 31, 23, 59),
            "end_time": datetime(2000, 1, 1, 0, 0),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
        },
        {
            "user_id": 500,
            "start_time": datetime(2000, 1, 1, 0, 0),
            "end_time": datetime(2000, 1, 1, 23, 59),
            "is_active": False,
            "is_focus": False,
            "is_break": False,
        },
        {
            "user_id": 500,
            "start_time": datetime(2000, 1, 1, 23, 59),
            "end_time": datetime(2000, 1, 2, 0, 0),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
        },
    ]

    assert data_in_collection == expected_data


def step_run_next_sessions_creation():
    user_id = 500
    activity_events = pd.Series([datetime(2000, 1, 2, 0, 5)])
    start_time = datetime(2000, 1, 2)
    tested_module.main(
        user_id=user_id, activity_events=activity_events, start_time=start_time
    )


def step_check_data2():
    collection = tested_module.ACTIVITY_SESSIONS_COLLECTION.find()

    data_in_collection = []
    for document in collection:
        data_in_collection.append(
            {key: value for key, value in document.items() if key != "_id"}
        )

    expected_data = [
        {
            "user_id": 500,
            "start_time": datetime(1999, 12, 31, 23, 59),
            "end_time": datetime(2000, 1, 1, 0, 0),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
        },
        {
            "user_id": 500,
            "start_time": datetime(2000, 1, 1, 0, 0),
            "end_time": datetime(2000, 1, 1, 23, 59),
            "is_active": False,
            "is_focus": False,
            "is_break": False,
        },
        {
            "user_id": 500,
            "start_time": datetime(2000, 1, 1, 23, 59),
            "end_time": datetime(2000, 1, 2, 0, 5),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
        },
    ]

    assert data_in_collection == expected_data


@test_steps(
    step_clear_activity_sessions_collection,
    step_run_main,
    step_check_data,
    step_run_next_sessions_creation,
    step_check_data2,
)
def test_main(test_step):
    test_step()


# initialize new collection
# perform activity_sessions creations stuff
# remove collection
