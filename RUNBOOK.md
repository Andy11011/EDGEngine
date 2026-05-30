# Table of Content

- [Update edgengine Container](#update-edgengine-container)
- [Check Nautilus Version](#check-nautilus-version)
- [Test New Indicators](#test-new-indicators)

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
  -e BINANCE_BAR_INTERVAL=1-WEEK \
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

```

This new section gives the user a quick way to verify the indicator is present and functioning inside the running container.
