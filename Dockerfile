FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (minimal). Add playwright browsers if needed.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Defaults (override via env):
# - ELPC_WEB_HOST (default 0.0.0.0)
# - ELPC_WEB_PORT (default 8000)
# - ELPC_DB       (default /data/prices.sqlite3)
CMD ["python", "-m", "el_price_checker.container_entrypoint"]
