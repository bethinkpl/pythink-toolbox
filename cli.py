import subprocess
from typing import Sequence

import click

from chronos import __version__
import chronos.settings


@click.group()
@click.version_option(version=__version__)
def main() -> None:
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
def run_api() -> None:
    """Starts API server."""
    run_args = [
        "uvicorn",
        "chronos.api.main:app",
        "--host",
        f"{chronos.settings.HOST_API}",
        "--port",
        "5000",
        "--debug",
        "--reload",
    ]

    subprocess.run(run_args, check=True)


main.add_command(ci)
main.add_command(run_api)
