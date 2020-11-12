from typing import Any

import nox

LOCATIONS = ["src", "tests", "noxfile.py", "cli.py"]

nox.options.sessions = ["update_env", "pre_commit", "all_tests", "pylint", "mypy"]


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
    session.run("pre-commit", "install", external=True)
    session.run("pre-commit", "run", "--all-files", external=True)


tests_base_args = ["--cov=src", "--cov-report", "html"]


@nox.session(python=False)
def all_tests(session: Any) -> None:
    """Test src & output coverage report.
    Report generation configuration in pyproject.toml & pytest.ini files.
    Usage:
        `nox -s tests [-- path]`
    """
    args = session.posargs or tests_base_args + ["."]
    session.run("pytest", *args, external=True)


@nox.session(python=False)
def unit_tests(session: Any) -> None:
    """Test src & output coverage report.
    Report generation configuration in pyproject.toml & pytest.ini files.
    Usage:
        `nox -s tests [-- path]`
    """
    args = session.posargs or tests_base_args + ["-m", "not integration", "."]
    session.run("pytest", *args, external=True)


@nox.session(python=False)
def integration_tests(session: Any) -> None:
    """Test src & output coverage report.
    Report generation configuration in pyproject.toml & pytest.ini files.
    Usage:
        `nox -s tests [-- path]`
    """
    args = session.posargs or tests_base_args + ["-m", "integration", "."]
    session.run("pytest", *args, external=True)


@nox.session(python=False)
def pylint(session: Any) -> None:
    """Run pylint fot linting whole codebase.
    Usage:
        `nox -s pylint [-- path]`
    """
    args = session.posargs or LOCATIONS
    session.run("pylint", *args, external=True)


@nox.session(python=False)
def mypy(session: Any) -> None:
    """Check type hints with mypy.
    Usage:
        `nox -s mypy [-- path]`
    """
    args = session.posargs or LOCATIONS
    session.run(
        "mypy",
        "--show-error-codes",
        "--config-file",
        "mypy.ini",
        "--strict",
        *args,
        external=True,
    )
