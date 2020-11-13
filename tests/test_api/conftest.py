# pylint: disable=import-outside-toplevel
import typing

import pytest
from starlette.testclient import TestClient


@pytest.fixture(scope="session")
def api_client() -> TestClient:
    """Get TestClient to test API."""
    from chronos.api.main import app

    return TestClient(app)


@pytest.fixture(scope="session")
def status_ok() -> int:
    return 200


@pytest.fixture(scope="session")
def headers() -> typing.Dict[str, str]:
    return {"Content-Type": "application/json"}
