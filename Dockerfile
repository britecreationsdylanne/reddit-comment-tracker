FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory for SQLite
RUN mkdir -p /app/data

# Set environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8080

# Run with gunicorn (1 worker to prevent duplicate scheduler jobs)
CMD exec gunicorn app:app --bind 0.0.0.0:$PORT --timeout 300 --workers 1
