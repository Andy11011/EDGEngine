# EdgeDesk Nautilus Apps

This folder contains two Nautilus-based Python apps:

- live_webhook_strategy.py (live signal app, webhook output)
- quickstart.py (local backtest demo)

The Docker image includes both apps and uses live_webhook_strategy.py as the default entrypoint.

## Local setup (optional)

python -m venv nautilus-env
nautilus-env\Scripts\activate
pip install -U pip
pip install -r requirements.txt

Run locally:

python live_webhook_strategy.py

or:

python quickstart.py

## Docker build

Build from this folder:

docker build -t edgedesk-nautilus .

## Docker run (main app: live webhook strategy)

You must pass your webhook URL.

docker run --rm \
 -e EDGEDESK_WEBHOOK_URL=<http://host.docker.internal:8000/webhook> \
 -e EDGEDESK_BINANCE_SYMBOL=BTCUSDT \
 -e EDGEDESK_BINANCE_ENV=LIVE \
 edgedesk-nautilus

Optional auth/rate-limit env vars:

- BINANCE_API_KEY
- BINANCE_API_SECRET
- EDGEDESK_WEBHOOK_BEARER_TOKEN

## Docker run quickstart.py

Use the same image, overriding the command:

docker run --rm edgedesk-nautilus python quickstart.py
