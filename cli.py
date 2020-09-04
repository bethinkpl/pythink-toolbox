import subprocess
from typing import Sequence

import click


@click.group()
def main() -> None:
    """\n😎 {{ cookiecutter.project_name }} CLI 😎\n"""


@click.command()
@click.argument("session", default="", type=str)
@click.argument("session_args", nargs=-1)
def ci(session: str, session_args: Sequence[str]) -> None:
    """Run Continuous Integration flow or part of it.\n
    Sessions defined in noxfile.py.\n
    Run `poetry run {{ cookiecutter.project_name }} ci [session]` to run particular CI session.
    Examples:
        - Run specific check:
            `poetry run {{ cookiecutter.project_name }} ci [check-name]`
        - Run specific test:
            `poetry run {{ cookiecutter.project_name }} ci tests [package].[module].[file]:[test_function]`
        - Run all checks but tests:
            `poetry run {{ cookiecutter.project_name }} ci "not tests"
    """

    run_args = ["poetry", "run", "nox"]
    if session:
        if session.startswith("not"):
            run_args += ["-k", f"{session}"]
        else:
            run_args += ["-s", session]
    if session_args:
        run_args += ["--", *session_args]

    subprocess.run(run_args)  # pylint: disable=subprocess-run-check


main.add_command(ci)
