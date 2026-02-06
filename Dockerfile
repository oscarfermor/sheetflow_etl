FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install -r requirements.txt

# Copy project files
COPY . .

# Create directories for logs and data
RUN mkdir -p /app/logs /app/data

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Default command - can be overridden
CMD ["python", "-m", "src.orchestator"]
