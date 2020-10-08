import uvicorn
from fastapi.applications import FastAPI

from chronos.api.routers.users_break_daily_time import users_break_daily_time_router
from chronos.api.routers.users_focus_daily_time import users_focus_daily_time_router
from chronos.api.routers.users_learning_daily_time import (
    users_learning_daily_time_router,
)
from chronos.api.routers.users_cumulative_learning_time import (
    users_cumulative_learning_time_router,
)
from chronos.logger import logger
from chronos.settings import HOST_API

logger.init_for_api()
app = FastAPI()

app.include_router(users_break_daily_time_router)
app.include_router(users_focus_daily_time_router)
app.include_router(users_learning_daily_time_router)
app.include_router(users_cumulative_learning_time_router)


if __name__ == "__main__":
    uvicorn.run(
        "chronos.api.main:app", host=HOST_API, port=5000, debug=True, reload=True
    )
