# pylint: disable=missing-function-docstring
from datetime import datetime

import pytest
import requests

from tests.consts import HEADERS, STATUS_OK
from tests.test_api.url_helpers import get_url

TEST_DATA = {
    "range-start": datetime(2000, 1, 1).isoformat(),
    "range-end": datetime(2021, 1, 1).isoformat(),
}

USER_ID = 299


@pytest.mark.e2e  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
@pytest.mark.skip(reason="https://bethink.atlassian.net/browse/LACE-465")  # type: ignore
def test_get_users_cumulative_learning_time() -> None:
    response = requests.get(
        get_url(f"users_cumulative_learning_time/{USER_ID}"),
        headers=HEADERS,
        params=TEST_DATA,
    )
    assert response.status_code == STATUS_OK
    assert response.json() == 650
