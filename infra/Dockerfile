FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system build deps required by some Python packages (psycopg2, numpy wheels may avoid compilation)
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
	   build-essential \
	   gcc \
	   libpq-dev \
	   curl \
	&& rm -rf /var/lib/apt/lists/*

# Copy requirements from the build context root (build with project root as context)
COPY requirements.txt /tmp/requirements.txt

# Upgrade pip and install Python requirements
RUN python -m pip install --upgrade pip setuptools wheel \
	&& pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files (adjust as needed) and set a sensible default command
COPY . /app

CMD ["/bin/bash"]
