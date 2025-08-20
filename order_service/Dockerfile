# -----------------
# Stage 1: Builder
# -----------------
FROM python:3.12-slim AS builder

WORKDIR /app

# Install only what’s needed for building Python deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip + add wheel for faster installs
RUN pip install --upgrade pip wheel setuptools

# Copy only dependency file first (for caching)
COPY requirements.txt .

# Install deps into a temporary folder
RUN pip install --no-cache-dir --default-timeout=100 \
    --target=/install -r requirements.txt

# Copy the full app source
COPY . .

# -----------------
# Stage 2: Final Image
# -----------------
FROM python:3.12-slim

WORKDIR /app

# Copy installed deps from builder (only site-packages)
COPY --from=builder /install /usr/local/lib/python3.12/site-packages

# Copy project source
COPY --from=builder /app /app

EXPOSE 8000

# Dev mode: use Django's runserver
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
