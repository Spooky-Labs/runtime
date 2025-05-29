FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user for security
RUN useradd -m trader && \
    mkdir -p /var/lib/trading-agent && \
    chown -R trader:trader /var/lib/trading-agent

# Switch to non-root user
USER trader

# Volume for persistence
VOLUME ["/var/lib/trading-agent"]

# Run the agent
CMD ["python", "runner.py"]