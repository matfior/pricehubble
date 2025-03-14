FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create necessary directories
RUN mkdir -p /app/data/output

# Set environment variables
ENV PYTHONPATH=/app

# Command to run when container starts
CMD ["prefect", "server", "start"] 