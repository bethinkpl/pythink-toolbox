import logging
import multiprocessing
import os

logger = logging.getLogger(__name__)

host = os.getenv("HOST", "0.0.0.0")
port = os.getenv("PORT", "8108")

workers_per_core = int(os.getenv("WORKERS_PER_CORE", 1))
use_max_workers = int(os.getenv("MAX_WORKERS") or 1)

cores = multiprocessing.cpu_count()
default_web_concurrency = workers_per_core * cores
workers = int(
    os.getenv("WEB_CONCURRENCY", min(max(default_web_concurrency, 2), use_max_workers))
)

# For debugging and testing
log_data = {
    "loglevel": os.getenv("LOG_LEVEL", "debug"),
    "workers": workers,
    "bind": os.getenv("BIND", f"{host}:{port}"),
    "graceful_timeout": int(os.getenv("GRACEFUL_TIMEOUT", 120)),
    "timeout": int(os.getenv("TIMEOUT", 120)),
    "keepalive": int(os.getenv("KEEP_ALIVE", 5)),
    "errorlog": os.getenv("ERROR_LOG", "-"),
    "accesslog": os.getenv("ACCESS_LOG", "-"),
    # Additional, non-gunicorn variables
    "workers_per_core": workers_per_core,
    "use_max_workers": use_max_workers,
    "host": host,
    "port": port,
}

logger.debug(log_data)
