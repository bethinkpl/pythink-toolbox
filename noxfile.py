from typing import Any

import nox

nox.options.sessions = ["pre_commit", "tests", "pylint", "mypy"]


@nox.session(python=False)
def update_env(session: Any) -> None:
    """Updates environment dependencies.

    Usage:
        `poetry run nox -s update_env`
    """
    session.run("poetry", "update", external=True)


@nox.session(python=False)
def pre_commit(session: Any) -> None:
    """Run pre-commit checks.

    Usage:
        `poetry run nox -s pre-commit`
    """
    session.run("poetry", "run", "pre-commit", "install", external=True)
    session.run(
        "poetry",
        "run",
        "pre-commit",
        "run",
        "--all-files",
        external=True,
    )


@nox.session(python=False)
def tests(session: Any) -> None:
    """Test src & output coverage report.
    Report generation configuration in pyproject.toml & pytest.ini files.

    Usage:
        `poetry run nox -s tests [-- path]`
    """
    args = session.posargs or ["--cov=src", "--cov-report", "html", "."]
    session.run("poetry", "run", "pytest", "-v", *args, external=True)


@nox.session(python=False)
def pylint(session: Any) -> None:
    """Run pylint fot linting whole codebase.

    Usage:
        `poetry run nox -s pylint [-- path]`
    """
    args = session.posargs or ["src", "tests"]
    session.run("poetry", "run", "pylint", *args, external=True)


@nox.session(python=False)
def mypy(session: Any) -> None:
    """Check type hints with mypy.

    Usage:
        `poetry run nox -s mypy [-- path]`
    """
    args = session.posargs or ["."]
    session.run(
        "poetry",
        "run",
        "mypy",
        "--show-error-codes",
        "--config-file",
        "mypy.ini",
        "--strict",
        *args,
        external=True,
    )
