ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}
ARG POETRY_VERSION=1.4.2

RUN apt-get update && apt-get install -y netcat git && apt-get autoremove -y
RUN pip install "poetry==${POETRY_VERSION}"

# Create unprivileged user
RUN adduser --disabled-password --gecos '' myuser

COPY wait_for_services.sh .
COPY /scripts scripts/
COPY setup.cfg .
COPY README.md .
COPY pyproject.toml .
COPY poetry.lock .
COPY .git .
COPY /schema_registry /schema_registry
COPY /tests /tests

# create a file in order to have coverage
RUN touch .coverage
RUN poetry install --no-interaction --no-ansi --all-extras

ENTRYPOINT ["./wait_for_services.sh"]
