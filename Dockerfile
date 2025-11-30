FROM gcr.io/the-farm-neutrino-315cd/base-with-models:0.1.5

WORKDIR /app

# Install procps for pgrep (needed by Kubernetes health checks)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user and copy models to staging location
# Models will be copied to user's home at runtime (needed for HuggingFace lock files)
RUN useradd -m trader && \
    mkdir -p /var/lib/trading-agent && \
    mkdir -p /opt/models/.cache && \
    mkdir -p /home/trader/.cache && \
    cp -r /root/.cache/huggingface /opt/models/.cache/ && \
    chown -R trader:trader /app /var/lib/trading-agent /opt/models/.cache/huggingface /home/trader/.cache

# Create entrypoint script to copy models from read-only staging to writable home
# Required because HuggingFace needs to write .lock files when loading models
RUN echo '#!/bin/sh' > /entrypoint.sh && \
    echo 'set -e' >> /entrypoint.sh && \
    echo '# Copy models from /opt (staged) to /home/trader/.cache (writable)' >> /entrypoint.sh && \
    echo 'cp -r /opt/models/.cache/huggingface /home/trader/.cache/' >> /entrypoint.sh && \
    echo '# Execute the CMD' >> /entrypoint.sh && \
    echo 'exec "$@"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Point HF_HOME to trader's writable directory
# Use offline mode to avoid network calls during runtime
ENV HF_HOME=/home/trader/.cache/huggingface
ENV HF_HUB_OFFLINE=1

# Switch to non-root user
USER trader

# Volume for persistence
VOLUME ["/var/lib/trading-agent"]

# Set entrypoint to copy models before running app
ENTRYPOINT ["/entrypoint.sh"]

# Run the agent
CMD ["python", "runner.py"]