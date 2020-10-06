FROM registry.gitlab.com/frankier/wikiparse

RUN rm -rf /app && mkdir /app

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN ~/.poetry/bin/poetry install
