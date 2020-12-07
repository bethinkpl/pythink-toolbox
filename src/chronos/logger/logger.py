import logging

import sentry_sdk

from chronos.settings import LOG_LEVEL, SENTRY_DSN_API


def init_for_api() -> None:
    """ Initializes sentry logger for API. """
    logging.getLogger(name="CHRONOS_API").setLevel(LOG_LEVEL)
    sentry_sdk.init(dsn=SENTRY_DSN_API)
