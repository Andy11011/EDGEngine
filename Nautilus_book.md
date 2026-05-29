# Table of Content

- [Rust Core Indicators](#rust-core-indicators)
- [Redis Streams](#redis-streams)
- [Nautilus Strategy Lifecycle and Data Flow](#nautilus-strategy-lifecycle-and-data-flow)

---

## Rust Core Indicators

### Architecture Overview: Rust + PyO3

NautilusTrader is built for **low‑latency, high‑frequency trading**. To achieve both speed and a user‑friendly Python interface, its core components – including technical indicators – are written in **Rust** and exposed to Python via **PyO3**.

- The Rust core lives in the `nautilus_core` repository (or the `crates/` directory in the main repo).
- Indicators are implemented as **Rust structs** with efficient, state‑ful update methods.
- **PyO3** generates Python bindings, allowing you to import and use these indicators as normal Python classes (e.g., `from nautilus_trader.indicators import ExponentialMovingAverage`).

### Why Rust? Performance and Safety

| Feature               | Benefit for Trading                                                |
|-----------------------|--------------------------------------------------------------------|
| **No Garbage Collector** | Predictable, low‑latency execution – no unexpected GC pauses.    |
| **Zero‑cost abstractions** | High‑level code compiles to fast machine code.                 |
| **Memory safety**     | Eliminates buffer overflows, use‑after‑free, and data races.        |
| **Concurrency**       | Safe multi‑threaded indicator updates (e.g., multiple symbols).     |

While you can write custom indicators in Python (great for prototyping), the built‑in Rust indicators are **10–100× faster** and designed for production.

### Example: Exponential Moving Average (EMA)

Let’s examine the EMA indicator – a core building block of many strategies.

#### Rust Implementation (simplified)

In the `nautilus_core` crate, the EMA is defined as:

```rust
// crates/indicators/src/ema.rs (simplified)
pub struct ExponentialMovingAverage {
    period: usize,
    alpha: f64,
    value: Option<f64>,
}

impl ExponentialMovingAverage {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            alpha: 2.0 / (period as f64 + 1.0),
            value: None,
        }
    }

    pub fn update(&mut self, price: f64) -> f64 {
        self.value = Some(match self.value {
            None => price,
            Some(prev) => (price - prev) * self.alpha + prev,
        });
        self.value.unwrap()
    }
}
```

#### PyO3 Binding

The binding file (`indicators.rs`) exposes the struct to Python:

```rust
#[pyclass]
pub struct ExponentialMovingAverage {
    inner: ema::ExponentialMovingAverage,
}

#[pymethods]
impl ExponentialMovingAverage {
    #[new]
    fn new(period: usize) -> Self {
        Self { inner: ema::ExponentialMovingAverage::new(period) }
    }

    fn update(&mut self, price: f64) -> f64 {
        self.inner.update(price)
    }

    #[getter]
    fn value(&self) -> Option<f64> {
        self.inner.value
    }
}
```

#### Using the Indicator in a Nautilus Strategy

In your Python strategy, you simply:

```python
from nautilus_trader.indicators import ExponentialMovingAverage

class MyStrategy(Strategy):
    def on_start(self) -> None:
        self.ema = ExponentialMovingAverage(period=20)
        self.register_indicator_for_bars(self.bar_type, self.ema)

    def on_bar(self, bar: Bar) -> None:
        # The indicator is automatically updated on each bar.
        if self.ema.value is not None:
            print(f"EMA value: {self.ema.value:.5f}")
```

Under the hood, Nautilus calls the Rust `update()` method for every bar, giving you nanosecond‑level performance.

### Using the Built‑in Indicator in a Strategy

The same pattern applies to all built‑in indicators:

- **Moving averages** (SMA, EMA, WMA, HMA, etc.)
- **Volatility bands** (Bollinger Bands, Donchian Channel, Keltner Channel)
- **Momentum oscillators** (RSI, Stochastic, MACD, etc.)

For example, to use the native **Donchian Channel** (instead of a custom Python version), you would write:

```python
from nautilus_trader.indicators import DonchianChannel

self.donchian = DonchianChannel(period=20)
self.register_indicator_for_bars(self.bar_type, self.donchian)
```

Then in `on_bar`, access `self.donchian.value` (the current middle band) or `self.donchian.upper` / `self.donchian.lower`. The signal logic (close > MA of middle) is not part of the indicator; you would implement that as a separate trading rule.

---

I've added a new section on Redis Streams to the Nautilus book, right after the Rust Core Indicators. Here's the updated table of contents and the new section.

---

## Redis Streams

### Why Redis Streams for Trading Systems

Real‑time trading systems often need to communicate with external services – dashboards, risk monitors, logging, or order management systems – without blocking the main event loop. **Redis Streams** offer an ideal solution:

- **Low latency**: Data stays in memory, so publishing a message takes microseconds.
- **Persistence**: Streams can be persisted to disk, allowing replay after a crash.
- **Consumer groups**: Multiple downstream services can read the same stream without duplication.
- **At‑least‑once delivery**: Acknowledge messages to ensure reliable processing.
- **Minimal overhead**: Redis is lightweight and battle‑tested in production at scale.

NautilusTrader does **not** include a built‑in Redis client, but integrating Redis is straightforward using the `redis-py` library. The strategy can publish events (e.g., regime changes, fills, signals) to a Redis stream, and external services can consume them.

### Example: Publishing Regime Changes from a Strategy

Assume you have a Donchian Channel strategy that detects bullish/bearish regimes. You want to push every regime change to a Redis stream so that a dashboard can display it in real time.

#### Step 1: Install redis-py and add to requirements

```bash
pip install redis
```

Add `redis` to your `requirements.txt`.

#### Step 2: Configure Redis connection in the strategy

In your strategy’s `__init__` or `on_start`, create a Redis client:

```python
import redis.asyncio as redis
import json

class DonchianRegimeStrategy(Strategy):
    def on_start(self) -> None:
        # ... existing code ...
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,
        )
```

#### Step 3: Publish a message when regime changes

Inside `on_bar`, when a regime change is detected, publish to a Redis stream:

```python
if regime != self._last_regime:
    self._last_regime = regime
    # Build the message payload
    payload = {
        "symbol": str(self.config.instrument_id),
        "regime": "BULLISH" if regime else "BEARISH",
        "upper": self.donchian.upper,
        "lower": self.donchian.lower,
        "ma": self.donchian.donchian_ma,
        "close": float(bar.close),
        "timestamp": bar.ts_event,
    }
    # Async publish to Redis stream (use run_coroutine_threadsafe if needed)
    asyncio.create_task(
        self.redis_client.xadd(
            "regime:btcusdt", 
            {"data": json.dumps(payload)},
            maxlen=1000  # keep last 1000 messages
        )
    )
```

If your strategy runs in a synchronous Nautilus thread, you may need to use `asyncio.run_coroutine_threadsafe` with a dedicated event loop. A simpler alternative is to use the synchronous `redis` client (not `asyncio`) in a separate thread or use `redis` with `blocking=False` to avoid stalling the strategy.

Here's a synchronous version (easier to integrate):

```python
import redis
import json

# In on_start
self.redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

# When regime changes
payload = {...}
self.redis_client.xadd("regime:btcusdt", {"data": json.dumps(payload)}, maxlen=1000)
```

The synchronous Redis client is non‑blocking for small writes (it uses sockets with timeouts), so it's safe to call directly from the strategy's `on_bar`.

### Consuming the Stream from an External Service

A separate service (e.g., a WebSocket bridge) can read the stream and forward updates to a frontend:

```python
import redis
import json
import asyncio
import websockets

r = redis.Redis(host="redis", port=6379, decode_responses=True)

async def stream_consumer(websocket, path):
    last_id = "0"
    while True:
        # Read new messages from the stream
        messages = r.xread({"regime:btcusdt": last_id}, block=1000, count=10)
        for stream, entries in messages:
            for entry_id, fields in entries:
                last_id = entry_id
                data = json.loads(fields["data"])
                await websocket.send(json.dumps(data))

async def main():
    async with websockets.serve(stream_consumer, "0.0.0.0", 8080):
        await asyncio.Future()

asyncio.run(main())
```

### Advantages Over Simple Pub/Sub

- **History**: New consumers can read past messages (e.g., the last 100 regime changes) – useful for dashboards that load late.
- **Acknowledgments**: Consumer groups allow you to confirm processing; if a bridge service crashes, another instance can take over from the last acknowledged message.
- **Rate limiting**: Streams are naturally bounded (with `MAXLEN`), preventing unbounded memory growth.

### Best Practices

1. **Use a separate Redis database** (e.g., `db=1`) for streams to avoid interfering with other caches.
2. **Keep messages small** – include only essential fields; avoid sending large arrays.
3. **Set `MAXLEN`** to cap stream size and avoid out‑of‑memory issues.
4. **Handle disconnections** – the Redis client will automatically reconnect, but your strategy should log errors and possibly buffer messages.
5. **Consider using `r.xadd` with a synchronous client** – it's fast enough for hundreds of thousands of messages per second.

### Full Example in a Nautilus Strategy

Below is a minimal complete example of a strategy that publishes regime changes to Redis Streams, assuming Redis is available at `redis:6379`.

```python
import os
import json
import redis
from nautilus_trader.trading.strategy import Strategy

class RedisRegimeStrategy(Strategy):
    def on_start(self):
        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,
        )
        self.subscribe_bars(self.bar_type)

    def on_bar(self, bar):
        # ... calculate regime ...
        if regime_changed:
            payload = {
                "symbol": str(self.instrument_id),
                "regime": "BULLISH" if regime else "BEARISH",
                "price": float(bar.close),
                "timestamp": bar.ts_event,
            }
            self.redis.xadd(
                f"regime:{self.instrument_id.symbol}",
                {"data": json.dumps(payload)},
                maxlen=1000,
            )
```

Now your dashboard or any other service can consume `regime:BTCUSDT` stream in real time.

---

## Nautilus Strategy Lifecycle and Data Flow

### The Big Picture

NautilusTrader is built around a **message-driven event loop**. Everything — bar updates, order fills, instrument data — flows through a central message bus. Your Python strategy sits at the end of that pipeline, receiving typed events via callback methods. Understanding exactly which callback fires when, and why, is essential for building reliable strategies.

### Actor vs Strategy

NautilusTrader has two base classes you can inherit from:

| Class | Use when |
|---|---|
| `Actor` | You only need data — no order management |
| `Strategy` | You need data **and** order management |

`Strategy` extends `Actor`, so it has all the same data callbacks plus execution callbacks (`on_order_filled`, `on_position_opened`, etc.). You can use `Strategy` even if you never place orders — it just means some callbacks are available but unused.

### Lifecycle: The State Machine

Every Actor/Strategy goes through these states in order:

```
INITIALIZED → STARTING → RUNNING → STOPPING → STOPPED → DISPOSED
```

The callbacks that map to these transitions:

```python
def on_start(self) -> None:
    # Called during STARTING — before the strategy receives any live data.
    # Safe to call request_bars(), subscribe_bars(), connect to Redis, etc.
    # The strategy is NOT yet RUNNING here.

def on_stop(self) -> None:
    # Called during STOPPING — clean up resources, cancel subscriptions.

def on_dispose(self) -> None:
    # Final cleanup — called once before the object is destroyed.
```

**Critical**: `on_start()` runs while the strategy is still in `STARTING` state, not `RUNNING`. This matters for `request_bars()` — see below.

### Data Subscriptions vs Data Requests

These are two completely different mechanisms and it is easy to confuse them.

#### `subscribe_bars(bar_type)` → `on_bar(bar)`

Subscribes to a **live stream** of bars going forward. Every time a new bar closes, `on_bar()` is called. This is the main event loop for a live strategy.

```python
def on_start(self) -> None:
    self.subscribe_bars(self.bar_type)

def on_bar(self, bar: Bar) -> None:
    print(f"New live bar: {bar.close}")
```

#### `request_bars(bar_type, start=...)` → `on_historical_data(data)`

Makes a **one-time async request** for historical bars. Results are delivered asynchronously via `on_historical_data()`. This is used to warm up indicators before live trading begins.

```python
def on_start(self) -> None:
    self.request_bars(self.bar_type, start=some_datetime)

def on_historical_data(self, data: Data) -> None:
    if isinstance(data, Bar):
        self.indicator.update(float(data.close))
```

### The `_warming_up` Guard Pattern

Use a flag to protect `on_bar` from processing bars before the indicator is ready:

```python
def __init__(self, config):
    super().__init__(config)
    self._warming_up: bool = True

def on_start(self) -> None:
    # ... fetch warmup bars synchronously ...
    self._warming_up = False
    self.subscribe_bars(self.bar_type)

def on_bar(self, bar: Bar) -> None:
    if self._warming_up:
        return
    # ... live logic ...
```

### Full Callback Reference

| Callback | Triggered by | Notes |
|---|---|---|
| `on_start()` | Node startup | Strategy is in `STARTING`, not `RUNNING` |
| `on_stop()` | Node shutdown | Clean up here |
| `on_bar(bar)` | `subscribe_bars()` | Live bars only |
| `on_historical_data(data)` | `request_bars()` | May not fire for EXTERNAL bars on live node |
| `on_data(data)` | Custom data types | Non-standard data published on the message bus |
| `on_instrument(instrument)` | Instrument updates | Fired when instrument definition changes |
| `on_quote_tick(tick)` | `subscribe_quote_ticks()` | Bid/ask tick |
| `on_trade_tick(tick)` | `subscribe_trade_ticks()` | Trade tick |
| `on_order_filled(event)` | Execution engine | Strategy only |
| `on_position_opened(event)` | Execution engine | Strategy only |

### `register_indicator_for_bars` — The Idiomatic Warmup

If you use a **native NautilusTrader indicator** (Rust-backed, from `nautilus_trader.indicators`), you can register it and the framework handles warmup automatically — no manual `update()` calls needed:

```python
from nautilus_trader.indicators import ExponentialMovingAverage

def on_start(self) -> None:
    self.ema = ExponentialMovingAverage(period=20)
    self.register_indicator_for_bars(self.bar_type, self.ema)
    self.request_bars(self.bar_type, start=some_datetime)
    self.subscribe_bars(self.bar_type)

def on_bar(self, bar: Bar) -> None:
    if not self.ema.initialized:
        return
    print(self.ema.value)
```

**This only works with native indicators.** Custom Python indicators (like a hand-rolled `DonchianChannel` class) cannot be registered this way and must be updated manually — either in `on_bar` or via the direct REST warmup pattern shown above.
