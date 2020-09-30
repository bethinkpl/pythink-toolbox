import json

import pytest
import requests

from tests.consts import HEADERS, STATUS_OK
from tests.test_api.url_helpers import get_url

TEST_DATA = {
    "users": [
        {"id": 299, "start_time": 15934399493, "end_time": 15934399490},
        {"id": 5994, "start_time": 15934399393, "end_time": 15934393490},
    ]
}


@pytest.mark.skip(reason="https://bethink.atlassian.net/browse/LACE-465")  # type: ignore
def test_get_users_learning_time() -> None:
    """
    Covers src.test_api.users_learning_time_daily.get_users_learning_time()
    """
    response = requests.post(
        get_url("learning_time"), headers=HEADERS, data=json.dumps(TEST_DATA)
    )
    assert response.status_code == STATUS_OK
    assert response.json() == {"299": 650, "5994": 650}
