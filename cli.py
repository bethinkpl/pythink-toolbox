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
@click.argument("session_args", nargs=-1)
@click.option("--environment", type=click.Choice(["local", "ci"]), default="local")
def ci(session: str, session_args: Sequence[str], environment: str) -> None:
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

    run_args = ["nox"]

    if environment == "local":
        run_args = ["docker", "exec", "-it", "chronos_base"] + run_args

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

    import logging

    from chronos.activity_sessions.main import main
    from chronos.activity_sessions.storage_operations import (
        read_last_generation_time_range_end,
    )
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

    last_generation_time = read_last_generation_time_range_end()

    if last_generation_time is None:
        _generate_from_scratch()
    else:
        main(time_range=TimeRange(start=last_generation_time, end=datetime.now()))


@click.command()
def generate_api_documentation() -> None:
    """
    Generates swagger documentation from json.
    """
    import json

    from chronos.api.main import app

    with open("docs/spec.js", "w") as file:
        file.write(f"var spec = {json.dumps(app.openapi()).strip()}\n")

    spec_url = "spec.js"
    title = f"{app.title} - Swagger UI"
    swagger_js_url = (
        "https://cdn.jsdelivr.net/npm/swagger-ui-dist@3.30.0/swagger-ui-bundle.js"
    )
    swagger_css_url = (
        "https://cdn.jsdelivr.net/npm/swagger-ui-dist@3.30.0/swagger-ui.css"
    )
    swagger_favicon_url = "https://fastapi.tiangolo.com/img/favicon.png"

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <link type="text/css" rel="stylesheet" href="{swagger_css_url}">
    <link rel="shortcut icon" href="{swagger_favicon_url}">
    <title>{title}</title>
    </head>
    <body>
    <div id="swagger-ui">
    </div>
    <script src="{swagger_js_url}"></script>
    <script src="{spec_url}"></script>
    <!-- `SwaggerUIBundle` is now available on the page -->
    <script>

    const ui = SwaggerUIBundle({{
        spec: spec,
        dom_id: '#swagger-ui',
        presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIBundle.SwaggerUIStandalonePreset
        ],
        layout: "BaseLayout",
        deepLinking: true,
        showExtensions: true,
        showCommonExtensions: true
    }})
    </script>
    </body>
    </html>
    """.strip()

    with open("docs/index.html", "w") as file:
        file.write(f"{html}\n")


cli_main.add_command(ci)
cli_main.add_command(generate_activity_sessions)
cli_main.add_command(generate_api_documentation)
