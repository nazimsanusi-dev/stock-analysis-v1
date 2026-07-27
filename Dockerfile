# ── Stage 1: Builder ──────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

# Non-interactive, no prompts, no cache
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /install

COPY requirements.txt .

RUN pip install --upgrade pip --quiet && \
    pip install --prefix=/install/packages -r requirements.txt --quiet


# ── Stage 2: Runtime ──────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Copy installed packages from builder
COPY --from=builder /install/packages /usr/local

WORKDIR /app

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash analyst && \
    mkdir -p /app/results && \
    chown -R analyst:analyst /app

COPY --chown=analyst:analyst analysis.py .

USER analyst

# Results volume mount point
VOLUME ["/app/results"]

CMD ["python", "analysis.py"]
