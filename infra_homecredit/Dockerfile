# Bookworm provides Java 17, which is supported by PySpark 3.5.x.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    curl \
    git \
    openjdk-17-jre-headless \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps from actual repo file
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

COPY config.py .
COPY cli ./cli
COPY platforms ./platforms
COPY log ./log

CMD ["python", "-m", "cli.main"]