FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Keep image lean while allowing wheels to install cleanly.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Installing nautilus_trader from fork's wheel (custom builds)
ARG NAUTILUS_WHEEL_URL=https://github.com/Andy11011/nautilus_trader/releases/download/fork-latest/nautilus_trader-1.228.0-cp312-cp312-manylinux_2_39_x86_64.whl
RUN pip install --no-cache-dir "${NAUTILUS_WHEEL_URL}"

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY EDGEngine.py ./
CMD ["python", "EDGEngine.py"]
