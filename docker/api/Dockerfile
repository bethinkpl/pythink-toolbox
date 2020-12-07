FROM python:3.8

EXPOSE 8108

RUN useradd gunicorn
ENV \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on

RUN pip install poetry

COPY ./docker/api/gunicorn_conf.py /gunicorn_conf.py

WORKDIR /chronos/
COPY ./pyproject.toml ./poetry.lock* /chronos/

COPY . /chronos

RUN poetry config virtualenvs.create false \
    && poetry install

RUN chown -R gunicorn:gunicorn /chronos \
    && chown gunicorn:gunicorn /gunicorn_conf.py

USER gunicorn

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "/gunicorn_conf.py", "src.chronos.api.main:app"]
