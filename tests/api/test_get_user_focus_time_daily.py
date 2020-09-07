import json
import requests

from tests.api.consts.api_tests import HEADERS, STATUS_OK
from tests.api.url_helpers import get_url

TEST_DATA = {"start_date": 15934399493, "end_date": 15934399490}

USER_ID = 299


def test_get_user_focus_time_daily():
    response = requests.post(
        get_url(f"focus_time_daily/{USER_ID}"),
        headers=HEADERS,
        data=json.dumps(TEST_DATA),
    )
    assert response.status_code == STATUS_OK
    assert response.json() == {"2000-01-01": 150, "2000-01-02": 20, "2000-01-03": 150}
