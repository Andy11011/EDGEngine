# Table of Content

- [Rust Core Indicators](#rust-core-indicators)

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
