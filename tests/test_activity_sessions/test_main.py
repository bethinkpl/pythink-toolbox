# pylint: disable=missing-function-docstring

from datetime import datetime
from typing import Callable, List, Dict, Union

import pytest
from pytest_mock import MockerFixture
from pythink_toolbox.testing.mocking import transform_function_to_target_string
import pandas as pd

import chronos.activity_sessions.main
from chronos.activity_sessions.activity_events import read


# TODO LACE-465 When GBQ integration ready -> replace mock/add new test
@pytest.mark.integration  # type: ignore[misc]
def test_main(
    mocker: MockerFixture,
    get_activity_session_collection_content_without_id: Callable[
        [], List[Dict[str, Union[int, datetime, bool]]]
    ],
    clear_activity_sessions_collection: Callable[[], None],
) -> None:

    mocker.patch(
        transform_function_to_target_string(read),
        return_value=pd.DataFrame(
            columns=["user_id", "client_time"],
            data=[
                [1, datetime(2000, 1, 1, 0, 1)],
                [1, datetime(2000, 1, 1, 0, 2)],
                [2, datetime(2000, 1, 1, 0, 1)],
            ],
        ),
    )

    clear_activity_sessions_collection()

    chronos.activity_sessions.main.main(start_time=mocker.ANY, end_time=mocker.ANY)

    data = get_activity_session_collection_content_without_id()

    expected_data = [
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 0, 2),
            "is_active": True,
            "is_break": False,
            "is_focus": False,
        },
        {
            "user_id": 2,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 0, 1),
            "is_active": True,
            "is_break": False,
            "is_focus": False,
        },
    ]

    assert data == expected_data

    clear_activity_sessions_collection()
