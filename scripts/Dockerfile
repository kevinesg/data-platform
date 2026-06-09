FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /uvx /bin/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY schemas ./schemas
COPY src ./src
COPY .env.example ./.env.example
COPY validate_config.py ./validate_config.py

ENTRYPOINT ["python", "src/personal_finance.py"]
CMD ["--help"]
