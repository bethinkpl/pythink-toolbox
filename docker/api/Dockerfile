FROM tiangolo/uvicorn-gunicorn:python3.8

RUN pip install poetry

WORKDIR /chronos

COPY . /chronos/

RUN poetry config virtualenvs.create false \
    && poetry install
