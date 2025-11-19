# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy harvester script
COPY harvester.py .

# Make harvester executable
RUN chmod +x harvester.py

# Create cache directory
RUN mkdir -p /app/cache

# Set cache directory as volume
VOLUME ["/app/cache"]

# Set default entry point
ENTRYPOINT ["python", "harvester.py"]

# Default command shows help
CMD ["--help"]
