FROM python:3.7-slim
    
RUN echo 'deb [check-valid-until=no] http://archive.debian.org/debian jessie-backports main' >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends apt-utils git

ENV PIP_FORMAT=legacy
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get install -y netcat && apt-get autoremove -y

# Create unprivileged user
RUN adduser --disabled-password --gecos '' myuser

WORKDIR /schema_registry/

COPY wait_for_services.sh .
COPY requirements.txt .

# create a file in order to have coverage
RUN touch .coverage

RUN pip3 install -r requirements.txt

ENTRYPOINT ["./wait_for_services.sh"]