import json
import requests

from consts.api_tests import HEADERS, STATUS_OK
from tests.api.url_helpers import get_url

test_data = {
    "start_date": 15934399493,
    "end_date": 15934399490
}

user_id = 299


def test_get_user_focus_time_daily():
    response = requests.post(get_url(f"focus_time_daily/{user_id}"), headers=HEADERS, data=json.dumps(test_data))
    assert response.status_code == STATUS_OK
    assert response.json() == {
        "2000-01-01": 150,
        "2000-01-02": 20,
        "2000-01-03": 150
    }
