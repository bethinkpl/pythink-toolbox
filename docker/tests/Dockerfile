FROM python:3.8

RUN pip install poetry

WORKDIR /chronos

COPY . /chronos/

RUN poetry config virtualenvs.create false \
    && poetry install

# to keep container running:
CMD tail -f /dev/null
