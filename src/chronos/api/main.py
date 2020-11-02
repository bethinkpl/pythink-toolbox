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

logger.init_for_api()
app = FastAPI()

app.include_router(users_break_daily_time_router)
app.include_router(users_focus_daily_time_router)
app.include_router(users_learning_daily_time_router)
app.include_router(users_cumulative_learning_time_router)
