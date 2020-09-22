import json

import pytest
import requests

from tests.consts import HEADERS, STATUS_OK
from tests.test_api.url_helpers import get_url

TEST_DATA = {"start_date": 15934399493, "end_time": 15934399490}

USER_ID = 299


@pytest.mark.skip(reason="https://bethink.atlassian.net/browse/LACE-465")  # type: ignore
def test_get_user_learning_time() -> None:
    """
    Covers src.test_api.user_learning_time.get_user_learning_time()
    """
    response = requests.post(
        get_url(f"learning_time/{USER_ID}"), headers=HEADERS, data=json.dumps(TEST_DATA)
    )
    assert response.status_code == STATUS_OK
    assert response.json() == {"299": 650}
