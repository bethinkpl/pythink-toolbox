from typing import Dict, Any

from fastapi import status
from fastapi.applications import FastAPI
from fastapi.openapi.utils import get_openapi

from chronos.api.routers.users_break_daily_time import users_break_daily_time_router
from chronos.api.routers.users_cumulative_learning_time import (
    users_cumulative_learning_time_router,
)
from chronos.api.routers.users_focus_daily_time import users_focus_daily_time_router
from chronos.api.routers.users_learning_daily_time import (
    users_learning_daily_time_router,
)
from chronos.logger import logger
from chronos import __version__

logger.init_for_api()
app = FastAPI()

app.include_router(users_break_daily_time_router)
app.include_router(users_focus_daily_time_router)
app.include_router(users_learning_daily_time_router)
app.include_router(users_cumulative_learning_time_router)


@app.get("/health", status_code=status.HTTP_200_OK)
def health() -> str:
    return "I'm alive"


def _custom_openapi() -> Dict[str, Any]:  # pragma: no cover
    """
    Add custom description to openapi schema.
    """
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Chronos API",
        version=__version__,
        description="Chronos FastAPI schema",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


setattr(app, "openapi", _custom_openapi)  # https://github.com/python/mypy/issues/2427
