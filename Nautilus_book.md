# Table of Content

- [Rust Core Indicators](#rust-core-indicators)
- [Redis Streams](#redis-streams)
- [Nautilus Strategy Lifecycle and Data Flow](#nautilus-strategy-lifecycle-and-data-flow)
- [Nautilus Sync Strategy](#nautilus-sync-strategy)
- [Integrating New Rust Core Indicator](#integrating-new-rust-core-indicator)

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

## Nautilus Sync Strategy

### The Problem with Forks

When you fork NautilusTrader and add custom code (like a Rust indicator), you face a recurring challenge: the upstream repo keeps moving — bug fixes, new adapters, performance improvements — while your fork drifts further behind with every commit you don't pull in.

The goal is a strategy that keeps your fork close to upstream with minimal friction, while your custom indicator stays intact.

### Recommended Approach: Sync from Upstream Default Branch

Rather than pinning to a specific tag or branch, the simplest approach for a personal fork with one custom indicator is:

- **Work on your fork's default branch** (whatever `github.com/Andy11011/nautilus_trader` shows by default)
- **Sync periodically** from upstream's `develop` branch when you want upstream fixes or improvements
- **Your custom indicator lives in your fork permanently** — it never goes to upstream

```
upstream/develop  ──────────────────────────────────►
                     │              │
                   sync           sync
                     │              │
your fork/develop ───▼──────────────▼──────────────►
                        + DonchianRegime (always present)
```

### Syncing: Two Ways

#### Option A — GitHub UI (easiest)

Go to `github.com/Andy11011/nautilus_trader`, click **"Sync fork"** → **"Update branch"**. Done in one click. GitHub merges upstream changes into your fork automatically.

Use this when there are no conflicts — which will almost always be the case since your changes (one new indicator) touch files upstream never touches.

#### Option B — Terminal

```bash
# One-time setup: add upstream as a remote
git remote add upstream https://github.com/nautechsystems/nautilus_trader

# Sync (do this whenever you want upstream changes)
git fetch upstream
git merge upstream/develop
git push origin
```

### When to Sync

You don't need to sync constantly. Sync when:

- A new NautilusTrader release fixes a bug you're hitting
- A new Binance adapter feature you need lands upstream
- Your fork's version is more than 2-3 releases behind

Don't sync just because upstream changed — unnecessary syncs are churn with no benefit.

### Conflict Risk

Your indicator touches exactly these files:

```
crates/indicators/src/volatility/donchian_regime.rs   ← new file
crates/indicators/src/volatility/mod.rs               ← one line added
crates/indicators/src/python/volatility/mod.rs        ← one line added
crates/indicators/src/python/mod.rs                   ← one line added
nautilus_trader/indicators/__init__.py                ← one line added
```

Upstream almost never touches `mod.rs` files in ways that conflict with an added `mod donchian_regime;` line. If a conflict does happen, it's always the same fix: keep both your line and the upstream changes in the `mod.rs` file.

### Versioning Your Wheel

Each time you sync and rebuild, the wheel version will match whatever upstream version is current (e.g. `1.228.0`). Your edgengine Dockerfile should install by **filename pattern**, not a hardcoded version, so it always picks up the latest built wheel:

```dockerfile
# In your edgengine Dockerfile
RUN pip install \
  "https://github.com/Andy11011/nautilus_trader/releases/download/fork-latest/nautilus_trader-$(curl -s https://github.com/Andy11011/nautilus_trader/releases/download/fork-latest/VERSION).whl"
```

Or simpler — just publish the release asset with a fixed tag name `fork-latest` that always gets overwritten on rebuild, so the URL never changes:

```dockerfile
RUN pip install \
  "https://github.com/Andy11011/nautilus_trader/releases/download/fork-latest/nautilus_trader-cp312-linux_x86_64.whl"
```

### Full Workflow Summary

```
1. Add your indicator to the fork (once)
        ↓
2. Push → GitHub Actions builds wheel → uploads to fork-latest release
        ↓
3. edgengine Dockerfile installs from fork-latest URL
        ↓
4. (weeks/months later) upstream releases something useful
        ↓
5. Click "Sync fork" in GitHub UI
        ↓
6. Push → wheel rebuilds automatically → edgengine picks it up on next deploy
```

The only manual step after initial setup is clicking "Sync fork" occasionally — everything else is automated.

---

## Integrating New Rust Core Indicator

Sometimes the built‑in indicators don’t cover your exact logic – you need a custom indicator, but you want it to run at Rust speed inside the Nautilus core. This guide walks you through adding a new Rust indicator, exposing it to Python via PyO3, and using it in a strategy.

We’ll use the **Enhanced Donchian Channel (EDC)** as our example – an indicator that tracks the Donchian Channel, its middle‑line moving average, and a regime signal (bullish/bearish) with crossover detection.

### Project Structure

NautilusTrader’s Rust code lives in the `crates/` directory:

```
crates/
├── indicators/
│   ├── src/
│   │   ├── volatility/
│   │   │   ├── mod.rs         ← exports the module
│   │   │   ├── edc.rs         ← your new indicator (Rust core)
│   │   ├── python/
│   │   │   ├── volatility/
│   │   │   │   ├── mod.rs     ← exports the Python bindings
│   │   │   │   ├── edc.rs     ← PyO3 bindings for your indicator
```

### Write the Rust Core Indicator

Create `crates/indicators/src/volatility/edc.rs`. Follow the pattern from `dc.rs`:

- Use **`f64`** for prices (the existing `DonchianChannel` uses `f64`).
- Use **`arraydeque::ArrayDeque`** for rolling buffers (maximum period 1024).
- Implement the `Indicator` trait (optional but recommended – it gives you `name()`, `has_inputs()`, `initialized()`, `reset()`, and `handle_bar()`).
- Expose `pub` fields for values so PyO3 getters can read them.

**Example (complete file):**

```rust
// crates/indicators/src/volatility/edc.rs

use std::fmt::Display;
use arraydeque::{ArrayDeque, Wrapping};
use nautilus_model::data::Bar;
use crate::indicator::Indicator;

const MAX_PERIOD: usize = 1_024;

#[repr(C)]
#[derive(Debug)]
#[cfg_attr(feature = "python", pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.indicators"))]
pub struct EnhancedDonchianChannel {
    pub donchian_period: usize,
    pub ma_period: usize,
    pub ma_type: MovingAverageType,
    pub use_close: bool,
    pub upper: f64,
    pub lower: f64,
    pub middle: f64,
    pub donchian_ma: f64,
    pub signal: Option<bool>,
    pub crossover: i8,
    pub initialized: bool,
    has_inputs: bool,
    // Rolling buffers
    highs: ArrayDeque<f64, MAX_PERIOD, Wrapping>,
    lows: ArrayDeque<f64, MAX_PERIOD, Wrapping>,
    closes: ArrayDeque<f64, MAX_PERIOD, Wrapping>,
    middle_buffer: ArrayDeque<f64, MAX_PERIOD, Wrapping>,
    ema_prev: Option<f64>,
    prev_upper: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovingAverageType {
    Sma,
    Ema,
}

impl Display for EnhancedDonchianChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(dc={}, ma={})", self.name(), self.donchian_period, self.ma_period)
    }
}

impl Indicator for EnhancedDonchianChannel {
    fn name(&self) -> String { stringify!(EnhancedDonchianChannel).to_string() }
    fn has_inputs(&self) -> bool { self.has_inputs }
    fn initialized(&self) -> bool { self.initialized }
    fn handle_bar(&mut self, bar: &Bar) {
        self.update_raw((&bar.high).into(), (&bar.low).into(), (&bar.close).into());
    }
    fn reset(&mut self) {
        self.highs.clear();
        self.lows.clear();
        self.closes.clear();
        self.middle_buffer.clear();
        self.upper = 0.0;
        self.lower = 0.0;
        self.middle = 0.0;
        self.donchian_ma = 0.0;
        self.signal = None;
        self.crossover = 0;
        self.ema_prev = None;
        self.prev_upper = None;
        self.has_inputs = false;
        self.initialized = false;
    }
}

impl EnhancedDonchianChannel {
    pub fn new(donchian_period: usize, ma_period: usize, ma_type: MovingAverageType, use_close: bool) -> Self {
        assert!(donchian_period > 0 && donchian_period <= MAX_PERIOD);
        assert!(ma_period > 0 && ma_period <= MAX_PERIOD);
        Self {
            donchian_period,
            ma_period,
            ma_type,
            use_close,
            upper: 0.0,
            lower: 0.0,
            middle: 0.0,
            donchian_ma: 0.0,
            signal: None,
            crossover: 0,
            highs: ArrayDeque::new(),
            lows: ArrayDeque::new(),
            closes: ArrayDeque::new(),
            middle_buffer: ArrayDeque::new(),
            ema_prev: None,
            prev_upper: None,
            has_inputs: false,
            initialized: false,
        }
    }

    pub fn update_raw(&mut self, high: f64, low: f64, close: f64) {
        // ... implementation (as shown in the full code) ...
    }
}

// ==================== PyO3 bindings ====================
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymethods]
impl EnhancedDonchianChannel {
    #[new]
    #[pyo3(signature = (donchian_period=20, ma_period=50, ma_type="EMA", use_close=true))]
    fn py_new(donchian_period: usize, ma_period: usize, ma_type: &str, use_close: bool) -> PyResult<Self> {
        let ma_type_enum = match ma_type.to_uppercase().as_str() {
            "SMA" => MovingAverageType::Sma,
            "EMA" => MovingAverageType::Ema,
            _ => return Err(pyo3::exceptions::PyValueError::new_err("ma_type must be 'SMA' or 'EMA'")),
        };
        Ok(Self::new(donchian_period, ma_period, ma_type_enum, use_close))
    }

    fn __repr__(&self) -> String {
        format!("EnhancedDonchianChannel(dc={}, ma={})", self.donchian_period, self.ma_period)
    }

    #[getter] fn name(&self) -> String { self.name() }
    #[getter] fn donchian_period(&self) -> usize { self.donchian_period }
    #[getter] fn ma_period(&self) -> usize { self.ma_period }
    #[getter] fn ma_type(&self) -> String {
        match self.ma_type { MovingAverageType::Sma => "SMA".to_string(), MovingAverageType::Ema => "EMA".to_string() }
    }
    #[getter] fn use_close(&self) -> bool { self.use_close }
    #[getter] fn upper(&self) -> f64 { self.upper }
    #[getter] fn lower(&self) -> f64 { self.lower }
    #[getter] fn middle(&self) -> f64 { self.middle }
    #[getter] fn donchian_ma(&self) -> f64 { self.donchian_ma }
    #[getter] fn signal(&self) -> Option<bool> { self.signal }
    #[getter] fn crossover(&self) -> i8 { self.crossover }
    #[getter] fn initialized(&self) -> bool { self.initialized }
    #[getter] fn has_inputs(&self) -> bool { self.has_inputs() }

    fn update_raw(&mut self, high: f64, low: f64, close: f64) {
        self.update_raw(high, low, close);
    }

    fn handle_bar(&mut self, bar: &Bar) {
        self.handle_bar(bar);
    }

    fn reset(&mut self) {
        self.reset();
    }
}
```

**Key points:**

- Use `f64` and `ArrayDeque` – the same as existing indicators.
- Implement the `Indicator` trait to integrate with Nautilus’s auto‑update mechanism.
- Place PyO3 methods **inside the same file** behind `#[cfg(feature = "python")]`.
- No separate bindings file is needed.

### Register the Module

**In `crates/indicators/src/volatility/mod.rs`**, add:

```rust
pub mod edc;
```

**In `crates/indicators/src/python/mod.rs`**, add the class to the list of exposed classes:

```rust
m.add_class::<crate::volatility::edc::EnhancedDonchianChannel>()?;
```

Make sure to add it after the other volatility classes (e.g., near `DonchianChannel`).

### (Optional) Add a Test Fixture

If you want to use the indicator in Rust unit tests, add a fixture in `crates/indicators/src/stubs.rs`:

```rust
#[fixture]
pub fn edc_10() -> EnhancedDonchianChannel {
    EnhancedDonchianChannel::new(10, 25, MovingAverageType::Ema, true)
}
```

### Update the Python `__init__.py`

In `nautilus_trader/indicators/__init__.py`, add the import:

```python
from nautilus_trader.indicators.volatility import EnhancedDonchianChannel
```

Also add `"EnhancedDonchianChannel"` to `__all__`.

**Important:** Do **not** create a `nautilus_trader/indicators/volatility/edc.py` file – the class is directly available in the compiled `volatility` module.

### Update Type Stubs (`.pyi` files)

For IDE support and type checking, add the class definition to both:

- `nautilus_trader/core/nautilus_pyo3.pyi` (if this file exists in your fork)
- `python/nautilus_trader/indicators/__init__.pyi`

The stub should mirror the public API of your indicator. Example:

```python
@typing.final
class EnhancedDonchianChannel:
    def __init__(
        self,
        donchian_period: int = 20,
        ma_period: int = 50,
        ma_type: str = "EMA",
        use_close: bool = True,
    ) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def donchian_period(self) -> int: ...
    @property
    def ma_period(self) -> int: ...
    @property
    def ma_type(self) -> str: ...
    @property
    def use_close(self) -> bool: ...
    @property
    def upper(self) -> float: ...
    @property
    def middle(self) -> float: ...
    @property
    def lower(self) -> float: ...
    @property
    def donchian_ma(self) -> float: ...
    @property
    def signal(self) -> bool | None: ...
    @property
    def crossover(self) -> int: ...
    @property
    def initialized(self) -> bool: ...
    @property
    def has_inputs(self) -> bool: ...
    def update_raw(self, high: float, low: float, close: float) -> None: ...
    def handle_bar(self, bar: model.Bar) -> None: ...
    def reset(self) -> None: ...
```
