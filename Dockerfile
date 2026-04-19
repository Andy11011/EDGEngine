FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Keep image lean while allowing wheels to install cleanly.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY live_webhook_strategy.py quickstart.py ./

# Main app is the live webhook strategy. quickstart.py is also present
# and can be run with: docker run --rm <image> python quickstart.py
CMD ["python", "live_webhook_strategy.py"]
