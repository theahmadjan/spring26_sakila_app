# Maintainer and version information
LABEL maintainer="Ahmad <ahmad@example.com>"
LABEL version="1.0.0"
LABEL description="Sakila Flask Application - Optimized"

# Stage 1: Builder
FROM python:3.9-slim AS builder

WORKDIR /app

# Copy ONLY requirements first for better layer caching
# (dependency layer only rebuilds when requirements.txt changes)
COPY requirements.txt .

# Install dependencies with no cache to reduce image size
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# Stage 2: Final minimal image
FROM python:3.9-slim

WORKDIR /app

# Copy installed packages from builder stage only
COPY --from=builder /install /usr/local

# Copy application code last
# (code changes won't bust the dependency cache)
COPY . .

# Create non-root user for security
# Running as root inside container is a security risk
RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app

USER appuser

# Only expose the port Flask actually uses
EXPOSE 5000

# Health check so Docker knows if app is actually working
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000')" || exit 1

# Run the application
CMD ["python", "app.py"]