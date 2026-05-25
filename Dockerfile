FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY src/ /app/src/
RUN pip install --no-cache-dir .

COPY dashboard/ /app/dashboard/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

EXPOSE 8000 8501
