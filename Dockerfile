FROM registry.gitlab.com/frankier/wikiparse

RUN apt-get update && apt-get install -y libopencc-dev

RUN rm -rf /app && mkdir /app

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN ~/.poetry/bin/poetry install

RUN python -c "from nltk import download as d; d('wordnet'); d('omw'); d('punkt')"
