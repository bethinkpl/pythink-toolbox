import json
import requests

from consts.api_tests import HEADERS, STATUS_OK
from tests.api.url_helpers import get_url

test_data = {
    "users": [
        {
          "id": 299,
          "start_date": 15934399493,
          "end_date": 15934399490
        },
        {
          "id": 5994,
          "start_date": 15934399393,
          "end_date": 15934393490
        }
    ]
}


def test_get_users_learning_time():
    response = requests.post(get_url('learning_time'), headers=HEADERS, data=json.dumps(test_data))
    assert response.status_code == STATUS_OK
    assert response.json() == {
        "users_learning_time":
        {
            "299": 304,
            "5994": 5999
        }
    }
