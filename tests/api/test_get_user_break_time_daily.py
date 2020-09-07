import json
import requests

from tests.api.consts.api_tests import HEADERS, STATUS_OK
from tests.api.url_helpers import get_url

TEST_DATA = {
    "start_date": 15934399493,
    "end_date": 15934399490
}

USER_ID = 299


def test_get_user_break_time_daily():
    response = requests.post(get_url(f"break_time_daily/{USER_ID}"), headers=HEADERS, data=json.dumps(TEST_DATA))
    assert response.status_code == STATUS_OK
    assert response.json() == {
        "2000-01-01": 200,
        "2000-01-02": 400,
        "2000-01-03": 50
    }
