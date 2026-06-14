# RIPE Atlas Ingester Container
# =============================
FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (for Docker caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the RIPE scripts (including the mapping JSON!)
COPY . .

# Run the 24/7 streaming pipeline
CMD ["python", "ripe_streaming_pipe.py"]