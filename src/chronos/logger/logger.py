import logging
import sentry_sdk
from chronos.settings import SENTRY_DSN_API, LOG_LEVEL


def init_for_api(name: str):
    logging.getLogger().setLevel(LOG_LEVEL)
    sentry_sdk.init(dsn=SENTRY_DSN_API)
