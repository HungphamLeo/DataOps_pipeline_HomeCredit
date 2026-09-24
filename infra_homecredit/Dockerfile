# Pin bookworm cứng để tránh Docker Hub re-tag sang Trixie (Debian 13 đã drop openjdk-17).
FROM python:3.11.13-slim-bookworm

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

# Copy config files and directories
COPY config.py .
COPY cli ./cli
COPY platforms ./platforms
COPY log ./log

CMD ["python", "-m", "cli.main"]
