# Stage 1 — builder: install all dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt


# Stage 2 — runtime: lean image with only what's needed
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime postgres lib only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy source code
COPY src/ ./src/
COPY data/ ./data/

EXPOSE 8501

CMD ["streamlit", "run", "src/dashboard.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]