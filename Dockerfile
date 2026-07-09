# syntax=docker/dockerfile:1.7

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

RUN useradd --create-home --shell /bin/bash app

WORKDIR /opt/bicam-collection

COPY --from=rotisserie / /opt/rotisserie
COPY pyproject.toml uv.lock README.md /opt/bicam-collection/
COPY src /opt/bicam-collection/src
COPY scripts /opt/bicam-collection/scripts
COPY specs /opt/bicam-collection/specs
COPY dbt_project /opt/bicam-collection/dbt_project
COPY api_schemas /opt/bicam-collection/api_schemas

RUN chown -R app:app /opt/bicam-collection /opt/rotisserie

USER app

RUN uv sync --frozen --no-dev

ENV PATH="/opt/bicam-collection/.venv/bin:${PATH}" \
    DBT_PROFILES_DIR=/opt/bicam-collection/dbt_project
