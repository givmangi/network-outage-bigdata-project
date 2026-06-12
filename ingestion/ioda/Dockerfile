# Ingester container
# ===================
# Slim Python 3.12 image. We install only what the ingestion
# script needs: requests (HTTP), kafka-python (Kafka producer),
# boto3 (S3/MinIO writes), and s3fs (optional: direct fsspec path writes).
#
# We use the 'slim' variant (not 'alpine') because kafka-python's
# C extension and boto3 have glibc dependencies that break on musl libc.

FROM python:3.12-slim

# # System packages:
# #   curl  - used by the healthcheck CMD and useful for debugging
# #   ca-certificates - needed for HTTPS calls to the IODA API
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     curl \
#     ca-certificates \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install dependencies first (separate layer = better cache).
# If requirements.txt doesn't change, Docker reuses this layer on rebuild.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code.
# Because we also mount ./ingester as a volume in docker-compose.yml,
# changes to .py files are reflected immediately without rebuilding —
# just restart the container.
COPY . .

# Default command: run the incremental polling loop.
# Configuration comes entirely from environment variables set in
# docker-compose.yml / .env, so no arguments are needed here.
CMD ["python", "run_loop.py"]