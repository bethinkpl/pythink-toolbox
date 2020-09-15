FROM tiangolo/uvicorn-gunicorn:python3.8-alpine3.10

COPY ./src /src
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml

RUN poetry install
