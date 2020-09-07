from importlib.metadata import version, PackageNotFoundError  # type: ignore[import]

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

# FIXME version to cli.py when LACE-446 merged
