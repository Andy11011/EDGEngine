# Table of Content

- [Update edgengine Container](#update-edgengine-container)
- [Check Nautilus Version](#check-nautilus-version)
- [Test New Indicators](#test-new-indicators)
- [Inspect Redis Streams](#inspect-redis-streams)
- [Reset Docker Images on Reboot](#reset-docker-images-on-reboot)
- [Query PostgreSQL Database](#query-postgresql-database)
- [Local Deploy](#local-deploy)
- [Testing with AWS SNS](#testing-with-aws-sns)

---

## Update edgengine Container

```bash
# 1. Pull the latest image
docker pull ghcr.io/andy11011/edgengine:latest

# 2. Stop and remove the old container
docker stop edgengine
docker rm edgengine

# 3. Run a new container with Redis environment variables
docker run -d \
  --name edgengine \
  --restart unless-stopped \
  --network edge-network \
  -e BINANCE_ENV=LIVE \
  -e BINANCE_SANDBOX=0 \
  -e BINANCE_SYMBOL=BTCUSDT \
  -e LOG_LEVEL=INFO \
  -e AWS_REGION=ap-southeast-1 \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  ghcr.io/andy11011/edgengine:latest

# 4. Check logs
docker logs edgengine -f
```

---

## Check Nautilus Version

You can verify which version of NautilusTrader is running in three ways:

**From the container logs** (easiest — it prints on every startup):

```
[INFO] EDGENGINE-001.TradingNode: nautilus_trader: 1.228.0
```

**From the terminal at any time:**

```bash
docker exec edgengine python -c "import nautilus_trader; print(nautilus_trader.__version__)"
```

**From inside Python code:**

```python
import nautilus_trader
print(nautilus_trader.__version__)
```

This is useful after rebuilding your fork's wheel and redeploying — confirm the version bumped from `1.227.0` to `1.228.0` to be sure the new wheel was actually picked up.

---

## Test New Indicators

After deploying the image with the new Rust‑based indicator, run this one‑liner inside the container to create the indicator and print its initial state:

```bash
docker exec edgengine python -c "from decimal import Decimal; from nautilus_trader.indicators import EnhancedDonchianChannel; edc = EnhancedDonchianChannel(20, 50, 'EMA', True); print(f'✅ Indicator created: {edc}')"
```

For a more thorough test that feeds sample bars and shows regime signals:

```bash
docker exec edgengine python -c "
from decimal import Decimal
from nautilus_trader.indicators import EnhancedDonchianChannel

edc = EnhancedDonchianChannel(20, 50, 'EMA', True)
bars = [
    (50000, 49000, 49500),
    (50200, 49800, 50100),
    (50500, 50000, 50300),
]

for high, low, close in bars:
    edc.update(Decimal(high), Decimal(low), Decimal(close))
    print(f'High={high} Low={low} Close={close} | Signal={edc.signal} Upper={edc.upper} Lower={edc.lower} MA={edc.donchian_ma} Crossover={edc.crossover}')
"
```

If the indicator works, you’ll see output like:

```
High=50000 Low=49000 Close=49500 | Signal=None Upper=None Lower=None MA=None Crossover=0
High=50200 Low=49800 Close=50100 | Signal=True Upper=50500 Lower=49000 MA=50000.0 Crossover=1
...
```

A `ModuleNotFoundError` or `AttributeError` means the new wheel wasn’t built correctly or the Python stub is missing.

---

## Inspect Redis Streams

Once the `edgengine` container is running, you can check the Redis streams that store regime changes or crossover signals.

### Connect to Redis CLI

```bash
sudo docker exec -it redis redis-cli
```

### List all keys (streams)

```bash
KEYS *
```

Example output:

```
1) "regime:BTCUSDT"
2) "signals:BTCUSDT"
```

### View regime changes (old version)

```bash
XRANGE regime:BTCUSDT - + COUNT 10
```

### View crossover signals (new version)

```bash
XRANGE signals:BTCUSDT - + COUNT 10
```

### Get stream metadata (length, first/last entry)

```bash
XINFO STREAM signals:BTCUSDT
```

### Monitor live signals (block until a new entry arrives)

```bash
XREAD BLOCK 0 STREAMS signals:BTCUSDT $
```

Press `Ctrl+C` to stop.

### Delete a stream (if you no longer need old data)

```bash
DEL regime:BTCUSDT
```

### One‑liner from host without entering the container

```bash
sudo docker exec -it redis redis-cli XRANGE signals:BTCUSDT - + COUNT 5
```

## Reset Docker Images on Reboot

To force a fresh pull of all container images on the next instance reboot (e.g., after a CloudFormation update or manual cleanup), follow these steps **before** rebooting:

```bash
# Stop all running containers
sudo docker stop edgedesk edgetrader postgres

# Remove all containers
sudo docker rm edgedesk edgetrader postgres

# Delete all Docker images (forces fresh pull next start)
sudo docker rmi -f $(sudo docker images -q)

# (Optional) Prune everything – volumes, networks, build cache
sudo docker system prune -a --volumes -f

# Reset cloud‑init state so that UserData runs again on next boot
sudo cloud-init clean --logs

# Reboot the instance
sudo reboot
```

After the reboot, your systemd scripts or CloudFormation user‑data will re‑pull the latest images and start the containers.  
If you want the images to be pulled **on every reboot** without manual cleanup, add a `docker pull ...` command to your startup script before the `docker run` lines.

## Query PostgreSQL Database

Connect to the PostgreSQL container and run SQL queries interactively:

```bash
sudo docker exec -it postgres psql -U user -d postgres
```

Replace `user` with the actual database user (if different). Once inside the `psql` prompt:

- List all tables: `\dt`
- Describe a table: `\d table_name`
- Run a query: `SELECT * FROM indicator_config;`
- Exit: `\q`

### Examples

```sql
-- Show all rows from the indicator_config table
SELECT * FROM indicator_config;

-- Filter by symbol
SELECT * FROM indicator_config WHERE symbol = 'BTCUSDT__15M';

-- See the JSON config data for a specific symbol
SELECT symbol, config_data FROM indicator_config WHERE symbol = 'BTCUSDT__15M';
```

### One‑off query from the host

If you prefer a single command without entering the interactive shell:

```bash
sudo docker exec -it postgres psql -U user -d postgres -c "SELECT symbol, updated_at FROM indicator_config;"
```

This is useful for quick checks or scripting.

## Local Deploy

### Prerequisites

- Docker Desktop installed and running.
- Build host: **Intel/AMD64** (the Nautilus wheel is x86_64-only, `manylinux_2_39`).
- Base image must be `python:3.12-slim-trixie` (glibc 2.39+) — plain `python:3.12-slim` defaults to bookworm (glibc 2.36) and the wheel install will fail.

### SQS Queue Setup (for `EVENT_DRIVEN` mode)

If you plan to run with `TRADE_SOURCE_MODE=EVENT_DRIVEN`, you need an **Amazon SQS queue** (Standard) to receive trade events.  
You do **not** need to create the SNS topic yet – that’s Step 2 of the integration plan. The queue URL is what the trader uses to poll for messages.

**Quick creation via AWS Console:**

1. Go to [Amazon SQS](https://console.aws.amazon.com/sqs/).
2. Click **“Create queue”**.
3. Under **Type**, select **Standard** (the default).
4. Enter a **Name**, e.g. `trade-events-queue`.
5. Leave all other settings as default (Visibility timeout 30s, retention 4 days, etc.).
6. Click **“Create queue”**.
7. After creation, click the queue name to open its details. Copy the **URL** shown at the top – that’s your `SQS_TRADE_EVENTS_QUEUE_URL`.

### Build

For virtual mainnet node:

```powershell
docker build -f docker/Dockerfile.binance_virtual_mainnet -t binance-virtual-mainnet-node:latest .
```

For live trading node:

```powershell
docker build -f docker/Dockerfile.binance_real -t binance-real-node:latest .
```

For api server:

```powershell
docker build -f docker/Dockerfile.api -t edge-point:latest .
```

Run from the directory containing `EdgeTrader/`, since `COPY EdgeTrader/...` paths are relative to the build context.

### Run

For virtual mainnet node:

```powershell
docker run --rm --env-file .env.local binance-virtual-mainnet-node:latest
```

For live trading node:

```powershell
docker run --rm --env-file .env.local binance-real-node:latest
```

For api server:

```powershell
docker run --rm --env-file .env.local -p 8000:8000 edge-point:latest
```

`--env-file` is required — host-shell env vars (`export`/`$env:`) are **not** automatically passed into the container.

Using the `-it` flags tells Docker to attach your terminal's standard input (stdin) and standard output (stdout) to the container's internal process, which allows you to view and interact with the pdb debugger prompt.

`-i` (interactive): Connects your terminal's stdin to the container so you can actually type commands (like n, s, p result) into the pdb prompt. Without this, pdb cannot read your keystrokes.

`-t` (tty): Allocates a pseudo-TTY (a terminal session) inside the container. This fixes the formatting, gives you text coloring, and prevents Python from crashing when it looks for a terminal screen to attach to.

```powershell
docker run --rm -it --env-file .env.local -p 8000:8000 edgetrader
```

### Environment variables reference

Only variables you are **likely to change** are listed below – all others have sensible defaults.

| Variable | Default | Purpose / Notes |
| --- | --- | --- |
| `BINANCE_SYMBOL` | `BTCUSDT` | Trading symbol |
| `TRADER_ID` | `EDGETRADER-001` | Nautilus trader ID (change if running multiple instances) |
| `BINANCE_ENV` | `LIVE` | `LIVE` or `TESTNET` – must match the API key environment |
| `BINANCE_SANDBOX` | `0` | `1` to use sandbox credential names (e.g., for testnet) |
| `AWS_REGION` | `ap-southeast-1` | AWS region – used for Secrets Manager, SQS, and credentials |
| `TRADING_MODE` | `VIRTUAL` | `VIRTUAL` (simulated execution), `TESTNET`, or `LIVE` |
| `DB_HOST` | `productiondb` | Postgres host. **Inside Docker, use `host.docker.internal`** to reach a local Postgres on your host. |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` / `DB_PORT` | `postgres` / `user` / `pass` / `5432` | Postgres connection details (change to match your local setup) |
| `TRADE_SOURCE_MODE` | `SINGLE` | `SINGLE` (legacy, one strategy) or `EVENT_DRIVEN` (new event‑sourced mode) |
| `SQS_TRADE_EVENTS_QUEUE_URL` | *(not set)* | **Required** when `TRADE_SOURCE_MODE=EVENT_DRIVEN`. The full URL of your SQS queue (from AWS Console). |

**Optional performance tuning** (defaults are usually fine):  
`SQS_POLL_WAIT_SECONDS` (default `20`), `SQS_MAX_MESSAGES` (default `10`).

---

### AWS credentials for SQS

When running in `EVENT_DRIVEN` mode, the trader needs credentials to poll the queue.  
You can provide them via environment variables (recommended):

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

**IAM permissions required:**  

- `sqs:ReceiveMessage`  
- `sqs:DeleteMessage`  
- `sqs:GetQueueAttributes` (optional, for monitoring)

For testing, you can attach the **`AmazonSQSFullAccess`** managed policy to your IAM user.

---

### Credential variables (Binance)

| Variable | Used when |
| --- | --- |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | HMAC credentials, live |
| `BINANCE_SANDBOX_API_KEY` / `BINANCE_SANDBOX_API_SECRET` | HMAC credentials, `BINANCE_SANDBOX=1` |
| `BINANCE_ED25519_PUBLIC_KEY` / `BINANCE_ED25519_PRIVATE_KEY` | Ed25519, only needed when `TRADING_MODE` is `TESTNET` or `LIVE` |

### Trading modes

| Mode | Behavior | Credentials needed |
| --- | --- | --- |
| `VIRTUAL` (default) | Live market data; orders simulated locally, nothing reaches Binance | HMAC only |
| `TESTNET` | Real orders to Binance Testnet | HMAC + Ed25519 (testnet), `BINANCE_ENV=TESTNET`, `BINANCE_SANDBOX=1` |
| `LIVE` | Real trading on mainnet | HMAC + Ed25519 (mainnet) |

### Setting credentials locally

**Linux / macOS:**

```bash
export BINANCE_API_KEY="your_api_key_here"
export BINANCE_API_SECRET="your_api_secret_here"
export BINANCE_ED25519_PUBLIC_KEY="your_binance_issued_api_key"
export BINANCE_ED25519_PRIVATE_KEY="$(cat /path/to/ed25519_private_key.pem)"
```

**Windows (PowerShell):**

```powershell
$env:BINANCE_API_KEY = "your_api_key_here"
$env:BINANCE_API_SECRET = "your_api_secret_here"
$env:BINANCE_ED25519_PUBLIC_KEY = "your_binance_issued_api_key"
$env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw C:\path\to\ed25519_private_key.pem
```

Preferred for Docker runs: put these in `.env.local` (add to `.gitignore`) and use `docker run --rm --env-file .env.local edgetrader`.

## Testing with AWS SNS

This section covers the commands to send test trade-event messages, verify queue depth, purge the queues, and reset the deduplication state – all useful for integration testing.

**Architecture note:** We are no longer sending directly to a single SQS queue. Per the deployment diagram, there's a central SNS topic (`trade-events`) that fans out to **two** per-venue SQS queues via subscription filter policies on a `venue` message attribute:

| `venue` attribute value | Routed to queue |
| --- | --- |
| `binance-real` | `binance-real-trade-events` |
| `binance-virtual-mainnet` | `binance-virtual-trade-events` |

Each node process still polls its own SQS queue exactly as before (nothing changes on the consumer side) — only how messages get *into* the right queue changes: publish once to SNS with the correct `venue` attribute, and the filter policy on each queue's subscription routes it to the right place.

**Sending a test Open message:**
Use the AWS CLI to publish to the SNS topic, with a `venue` message attribute so the filter policy routes it to the right queue. The consumer still expects the same schema — `ticker`, `event_type` (open/cancel), and `occurred_at` as the deduplication key.

```bash
aws sns publish `
  --topic-arn "arn:aws:sns:ap-southeast-1:298724921728:trade-events" `
  --region ap-southeast-1 `
  --message file://open_message.json `
  --message-attributes file://virtual_message_attrs.json
```

```bash
aws sns publish `
  --topic-arn "arn:aws:sns:ap-southeast-1:298724921728:trade-events" `
  --region ap-southeast-1 `
  --message file://cancel_message.json `
  --message-attributes file://virtual_message_attrs.json
```

```bash
aws sns publish `
  --topic-arn "arn:aws:sns:ap-southeast-1:298724921728:trade-events" `
  --region ap-southeast-1 `
  --message file://open_message.json `
  --message-attributes file://real_message_attrs.json
```

```bash
aws sns publish `
  --topic-arn "arn:aws:sns:ap-southeast-1:298724921728:trade-events" `
  --region ap-southeast-1 `
  --message file://cancel_message.json `
  --message-attributes file://real_message_attrs.json
```

Sample `open_message.json` (unchanged from before):

```json
{
  "ticker": "ATMUSDT.BINANCE",
  "event_type": "open",
  "occurred_at": "2026-08-07T10:00:00.123Z",
  "side": "BUY",
  "ep": 1.598,
  "sl": 1.587,
  "tp": 1.618
}
```

For a **Cancel** message (no extra parameters):

```json
{
  "ticker": "ATMUSDT.BINANCE",
  "event_type": "cancel",
  "occurred_at": "2026-08-07T10:15:00.456Z"
}
```

⚠️ **Get the `venue` attribute value exactly right.** It must match the subscription filter policy string exactly (`binance-real` or `binance-virtual-mainnet`) — a typo or the wrong value means SNS silently drops the message for that subscription (no error, it just never reaches either queue). If a test message seems to vanish, this is the first thing to check.

**Checking queue depth** – to see how many messages are waiting on each per-venue queue:

```bash
aws sqs get-queue-attributes \
  --queue-url "https://sqs.ap-southeast-1.amazonaws.com/298724921728/binance-real-trade-events" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  --region ap-southeast-1

aws sqs get-queue-attributes \
  --queue-url "https://sqs.ap-southeast-1.amazonaws.com/298724921728/binance-virtual-trade-events" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  --region ap-southeast-1
```

**Pruning the SQS queues (delete all messages):**
To completely empty a queue, including in-flight messages, use `purge-queue`. This is irreversible and takes up to 60 seconds. Run it per queue — purging one does not affect the other.

```bash
aws sqs purge-queue \
  --queue-url "https://sqs.ap-southeast-1.amazonaws.com/298724921728/binance-real-trade-events" \
  --region ap-southeast-1

aws sqs purge-queue \
  --queue-url "https://sqs.ap-southeast-1.amazonaws.com/298724921728/binance-virtual-trade-events" \
  --region ap-southeast-1
```

**Resetting the deduplication ledger (`claimed_events`):**
Even after purging the queues, the consumer will skip messages whose `(ticker, event_type, occurred_at)` keys are already stored in the `claimed_events` table. To allow reprocessing (e.g., after a bug fix or for repeated tests), truncate or conditionally delete from that table:

```bash
# Truncate the entire table (clears all claimed keys)
docker exec -it postgres psql -U user -d postgres -c "TRUNCATE TABLE claimed_events;"

# Or delete only for a specific ticker
docker exec -it postgres psql -U user -d postgres -c "DELETE FROM claimed_events WHERE ticker = 'ATMUSDT.BINANCE';"
```

⚠️ **Caution**: Truncating or deleting from `claimed_events` will cause **all** messages still in either queue (or redelivered from a DLQ) to be processed again – safe for testing but not for production.

**Viewing messages in the AWS Console:**
Messages are still consumed from SQS, so use the SQS console as before — navigate to the relevant queue (`binance-real-trade-events` or `binance-virtual-trade-events`), click **"Send and receive messages"**, then **"Poll for messages"** to inspect up to 10 visible messages. Expand a message to see its full body and attributes. To check the SNS side (e.g. confirm a subscription's filter policy, or see delivery failures), go to the SNS console, open the `trade-events` topic, and check its **Subscriptions** tab.

**Message retry and Dead‑Letter Queue:**
Unchanged — this happens at the SQS level, per queue, regardless of whether the message arrived via SNS or a direct send. If the consumer fails to process a message (e.g., throws an exception), the message is **not deleted** and becomes visible again after the visibility timeout (default 30 seconds). After `maxReceiveCount` failures, it moves to that queue's configured Dead‑Letter Queue (DLQ). You can manually move messages from the DLQ back to the main queue via the console or CLI for re‑testing.
