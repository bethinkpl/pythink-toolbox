# pylint: disable=missing-function-docstring
from typing import Dict

from starlette.testclient import TestClient


def test_health(
    api_client: TestClient, status_ok: int, headers: Dict[str, str]
) -> None:

    response = api_client.get("/health", headers=headers)

    assert response.status_code == status_ok
    assert response.json() == "I'm alive"
