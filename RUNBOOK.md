# Table of Content

- [Update edgengine Container](#update-edgengine-container)
- [Check Nautilus Version](#check-nautilus-version)

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
