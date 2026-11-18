FROM python:3.12.16-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY src/ ./src/
COPY config/ ./config/

RUN pip install --no-cache-dir .

FROM base AS test

RUN pip install --no-cache-dir ".[dev]"

COPY tests/ ./tests/

CMD ["sh", "-c", "ruff check src tests && pytest -q"]

FROM base AS runtime

RUN groupadd --system app \
    && useradd --system --gid app --no-create-home app

USER app

ENTRYPOINT ["weather-ingestion"]
