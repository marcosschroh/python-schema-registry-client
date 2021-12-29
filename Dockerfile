ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}
    
RUN apt-get update && apt-get install -y netcat && apt-get autoremove -y

# Create unprivileged user
RUN adduser --disabled-password --gecos '' myuser

WORKDIR /schema_registry/

COPY wait_for_services.sh setup.py README.md /scripts setup.cfg pyproject.toml ./

# create a file in order to have coverage
RUN touch .coverage

RUN ./install

ENTRYPOINT ["./wait_for_services.sh"]
