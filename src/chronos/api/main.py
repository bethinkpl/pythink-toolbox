import uvicorn
from fastapi.applications import FastAPI

from chronos.api.routers.user_break_time_daily import user_break_time_daily_router
from chronos.api.routers.user_focus_time_daily import user_focus_time_daily_router
from chronos.api.routers.user_learning_time_daily import user_learning_time_daily_router
from chronos.api.routers.user_cumulative_learning_time import (
    user_cumulative_learning_time_router,
)

from chronos.logger import logger

logger.init_for_api()
app = FastAPI()

app.include_router(user_break_time_daily_router)
app.include_router(user_focus_time_daily_router)
app.include_router(user_learning_time_daily_router)
app.include_router(user_cumulative_learning_time_router)


if __name__ == "__main__":
    uvicorn.run(
        "chronos.api.main:app", host="127.0.0.1", port=5000, debug=True, reload=True
    )
