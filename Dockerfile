# Data Catalog Service - Dockerfile.

FROM python:3.11-slim AS builder

# Some labels are defined to store metadata.
LABEL image_version="1.3.1"
LABEL app_version="1.3.1"
LABEL maintainer="Ignacio Domínguez Martínez-Casanueva"

# Kudos to https://github.com/gianfa/poetry/blob/docs/docker-best-practices/docker-examples/poetry-multistage/Dockerfile

# --- Install Poetry ---
ARG POETRY_VERSION=1.8

ENV POETRY_HOME=/opt/poetry
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Tell Poetry where to place its cache and virtual environment
ENV POETRY_CACHE_DIR=/opt/.cache

RUN pip install "poetry==${POETRY_VERSION}"

WORKDIR /app

# --- Reproduce the environment ---
# You can comment the following two lines if you prefer to manually install
#   the dependencies from inside the container.
COPY pyproject.toml .

# Install the dependencies and clear the cache afterwards.
#   This may save some MBs.
RUN poetry install --no-root && rm -rf $POETRY_CACHE_DIR

# Now let's build the runtime image from the builder.
#   We'll just copy the env and the PATH reference.
FROM python:3.11-slim AS runtime

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
# Copy fastapi source code
COPY ./data_catalog_service /app/data_catalog_service
WORKDIR /app/data_catalog_service

ENTRYPOINT [ "fastapi" ]
# Execute FastAPI in run mode (production) by default
CMD ["run", "main.py", "--port", "80"]
