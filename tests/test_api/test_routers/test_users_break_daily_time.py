# pylint: disable=missing-function-docstring
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from tests.consts import HEADERS, STATUS_OK

TEST_DATA = {
    "range-start": datetime(2000, 1, 1).isoformat(),
    "range-end": datetime(2021, 1, 1).isoformat(),
}

USER_ID = 299


@pytest.mark.e2e  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
def test_get_users_break_daily_time(api_client: TestClient) -> None:
    response = api_client.get(
        f"/users_break_daily_time/{USER_ID}", headers=HEADERS, params=TEST_DATA
    )
    assert response.status_code == STATUS_OK

    for date, duration_ms in response.json():
        assert isinstance(date, str)
        assert isinstance(date, int)
