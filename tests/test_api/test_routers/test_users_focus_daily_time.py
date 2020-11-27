# pylint: disable=missing-function-docstring
from datetime import datetime
from typing import Dict

import pytest
from starlette.testclient import TestClient

TEST_DATA = {
    "range-start": datetime(2000, 1, 1).isoformat(),
    "range-end": datetime(2021, 1, 1).isoformat(),
}

USER_ID = 299


@pytest.mark.integration
def test_get_users_focus_daily_time(
    api_client: TestClient, status_ok: int, headers: Dict[str, str]
) -> None:
    response = api_client.get(
        f"users_focus_daily_time/{USER_ID}", headers=headers, params=TEST_DATA
    )
    assert response.status_code == status_ok
    assert isinstance(response.json(), list)
