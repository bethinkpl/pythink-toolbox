import json

import pytest
import requests

from tests.consts import HEADERS, STATUS_OK
from tests.test_api.url_helpers import get_url

TEST_DATA = {"start_time": 15934399493, "end_time": 15934399490}

USER_ID = 299


@pytest.mark.skip(reason="https://bethink.atlassian.net/browse/LACE-465")  # type: ignore
def test_get_users_learning_daily_time() -> None:
    """
    Covers src.test_api.users_learning_daily_time.get_users_learning_daily_time()
    """
    response = requests.post(
        get_url(f"learning_time_daily/{USER_ID}"),
        headers=HEADERS,
        data=json.dumps(TEST_DATA),
    )
    assert response.status_code == STATUS_OK
    assert response.json() == {"2000-01-01": 100, "2000-01-02": 300, "2000-01-03": 250}
