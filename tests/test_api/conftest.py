# pylint: disable=import-outside-toplevel

import pytest
from starlette.testclient import TestClient


@pytest.fixture(scope="session")
def api_client() -> TestClient:
    """Get TestClient to test API."""
    from chronos.api.main import app

    return TestClient(app)
