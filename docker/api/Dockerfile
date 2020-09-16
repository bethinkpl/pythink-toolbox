FROM tiangolo/uvicorn-gunicorn:python3.8

RUN pip install poetry

COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml

RUN poetry config virtualenvs.create false \
    && poetry install

COPY ./src /src
