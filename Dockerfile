FROM python:3.11.4-slim as builder

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY ./pyproject.toml .
COPY ./poetry.lock .
COPY ./README.md .

ENV POETRY_HOME /opt/poetry
ENV POETRY_VIRTUALENVS_IN_PROJECT true

RUN python3 -m venv $POETRY_HOME \
    && $POETRY_HOME/bin/pip install poetry==1.4.0 \
    && $POETRY_HOME/bin/poetry --version

RUN $POETRY_HOME/bin/poetry install --no-root

FROM python:3.11.4-slim

ENV PYTHONUNBUFFERED True

ENV VENV_HOME /opt/.venv
COPY --from=builder /app/.venv $VENV_HOME
ENV PATH="$VENV_HOME/bin:$PATH"

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . .

CMD ["python", "-m", "main"]

