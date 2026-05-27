# Table of Content

- [Update edgengine Container](#update-edgengine-container)

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

> **Note:** This assumes a Redis container named `redis` is already running on the same Docker network (e.g., `edge-network`). If not, add `--network edge-network` to the `docker run` command.
