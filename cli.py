# pylint: disable=import-outside-toplevel
import subprocess
from typing import Sequence
from datetime import datetime

import click

from chronos import __version__


@click.group()
@click.version_option(version=__version__)
def cli_main() -> None:
    """\n😎 CHRONOS CLI 😎\n"""


@click.command()
@click.argument("session", default="", type=str)
@click.argument("session_args", nargs=-1)
def ci(session: str, session_args: Sequence[str]) -> None:
    """Run Continuous Integration flow or part of it.\n
    Sessions defined in noxfile.py.\n
    Run `poetry run chronos ci [session]` to run particular CI session.
    Examples:
        - Run specific check:
            `poetry run chronos ci [check-name]`
        - Run specific test:
            `poetry run chronos ci tests [package].[module].[file]:[test_function]`
        - Run all checks but tests:
            `poetry run chronos ci "not tests"
    """

    run_args = ["poetry", "run", "nox"]
    if session:
        if session.startswith("not"):
            run_args += ["-k", f"{session}"]
        else:
            run_args += ["-s", session]
    if session_args:
        run_args += ["--", *session_args]

    subprocess.run(run_args, check=True)


@click.command()
def generate_activity_sessions() -> None:
    """Generates activity_sessions from time of last generation to now
    and updates materialized views."""

    from chronos.activity_sessions.main import main
    from chronos.activity_sessions.storage_operations import (
        read_last_generation_time_range_end,
        TimeRange,
    )

    last_generation_time = read_last_generation_time_range_end() or datetime(1970, 1, 1)

    main(time_range=TimeRange(start=last_generation_time, end=datetime.now()))


@click.command()
def run_api() -> None:
    """Starts API server."""
    from uvicorn import run

    import chronos.settings

    run(
        "chronos.api.main:app",
        host=chronos.settings.HOST_API,
        port=5000,
        debug=True,
        reload=True,
    )


cli_main.add_command(ci)
cli_main.add_command(generate_activity_sessions)
cli_main.add_command(run_api)
