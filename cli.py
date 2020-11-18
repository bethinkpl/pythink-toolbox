# pylint: disable=import-outside-toplevel
import subprocess
from typing import Sequence
from datetime import datetime, timedelta

import click

from chronos import __version__


@click.group()
@click.version_option(version=__version__)
def cli_main() -> None:
    """\n😎 CHRONOS CLI 😎\n"""


@click.command()
@click.argument("session", default="", type=str)
@click.argument("other_args", nargs=-1)
@click.option("-v", "--verbose", count=True)
def ci(session: str, other_args: Sequence[str], verbose: int) -> None:
    """Run Continuous Integration flow or part of it.\n
    Sessions defined in noxfile.py.\n
    Run `poetry run cli ci [session]` to run particular CI session.
    Examples:
        - Run specific check:
            `poetry run cli ci [check-name]`
        - Run specific test:
            `poetry run cli ci tests [package].[module]:[test_function]`
        - Run all checks but tests:
            `poetry run cli ci "not tests"
    """
    docker_compose_args = ["docker-compose", "-f", "docker-compose-ci.yml"]

    run_args = docker_compose_args + ["run", "--rm", "chronos-ci"]

    if session:
        run_args += ["nox"]
        if session.startswith("not"):
            run_args += ["-k", f"{session}"]
        else:
            run_args += ["-s", session]

    if other_args:
        run_args += ["--", f"-{'v'*verbose}", *other_args]

    try:
        click.secho(f"RUN: {' '.join(run_args)}", fg="cyan", bold=True)
        subprocess.run(run_args, check=True)
    except subprocess.CalledProcessError as err:
        raise err
    finally:
        down_args = docker_compose_args + ["down", "-v"]
        click.secho(f"RUN: {' '.join(down_args)}", fg="cyan", bold=True)
        subprocess.run(down_args, check=True)


@click.command()
def generate_activity_sessions() -> None:
    """Generates activity_sessions from time of last generation to now
    and updates materialized views."""

    import logging

    from pymongo import DESCENDING

    from chronos.activity_sessions.main import main
    from chronos.storage import mongo_specs
    from chronos.custom_types import TimeRange

    def _generate_from_scratch() -> None:
        logging.info("Generating activity sessions from scratch")

        start_date = datetime(2019, 8, 11)  # 13 Aug 2019 - date of table creation
        chunk_size = timedelta(days=100)

        time_range = TimeRange(start=start_date, end=start_date + chunk_size)

        while time_range.end < datetime.now():

            main(time_range=time_range)

            time_range = TimeRange(
                start=time_range.end, end=time_range.end + chunk_size
            )

    time_range_end = mongo_specs.collections.generations.find_one(
        projection={"_id": False, "time_range.end": True},
        sort=[("time_range.end", DESCENDING)],
    )

    try:
        last_generation_time = time_range_end["time_range"]["end"]
    except KeyError:
        _generate_from_scratch()
    else:
        main(time_range=TimeRange(start=last_generation_time, end=datetime.now()))

    # FIXME test it


cli_main.add_command(ci)
cli_main.add_command(generate_activity_sessions)
