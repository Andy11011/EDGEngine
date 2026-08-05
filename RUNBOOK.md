# Table of Content

- [Update edgengine Container](#update-edgengine-container)
- [Check Nautilus Version](#check-nautilus-version)
- [Test New Indicators](#test-new-indicators)
- [Inspect Redis Streams](#inspect-redis-streams)
- [Reset Docker Images on Reboot](#reset-docker-images-on-reboot)
- [Query PostgreSQL Database](#query-postgresql-database)
- [Local Deploy](#local-deploy)

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

```powershell
docker build -f Dockerfile.trader -t edgetrader .
```

Run from the directory containing `EdgeTrader/`, since `COPY EdgeTrader/...` paths are relative to the build context.

### Run

```powershell
docker run --rm --env-file .env.local edgetrader
```

`--env-file` is required — host-shell env vars (`export`/`$env:`) are **not** automatically passed into the container.

### Environment variables reference

| Variable | Default | Purpose |
| --- | --- | --- |
| `BINANCE_SYMBOL` | `BTCUSDT` | Trading symbol |
| `TRADER_ID` | `EDGETRADER-001` | Nautilus trader ID |
| `BINANCE_ENV` | `LIVE` | `LIVE` or `TESTNET` — must match the environment the API key was issued for |
| `BINANCE_BAR_INTERVAL` | `15-MINUTE` | Bar aggregation interval |
| `LOG_LEVEL` | `INFO` | Nautilus log level |
| `BINANCE_SANDBOX` | `0` | `1` to use sandbox credential names |
| `AWS_REGION` | `ap-southeast-1` | Region for AWS Secrets Manager and SQS |
| `TRADING_MODE` | `VIRTUAL` | `VIRTUAL` (simulated exec), `TESTNET`, or `LIVE` |
| `SANDBOX_STARTING_BALANCES` | `10000 USDT,1 BTC` | Starting balances for `VIRTUAL` mode |
| `SANDBOX_ACCOUNT_TYPE` | `CASH` | Account type for `VIRTUAL` mode |
| `DB_HOST` | `productiondb` | Postgres host; won't resolve locally unless overridden |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` / `DB_PORT` | `postgres` / `user` / `pass` / `5432` | Postgres connection details |

**Credential variables:**

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
