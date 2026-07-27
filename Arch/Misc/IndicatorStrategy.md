# Indicator Calculation Strategy: EdgeDesk vs Nautilus Backend

Date: 2026-04-23  
Context: Architectural decision for how a shared set of core indicators should be managed across EdgeDesk and a nautilus-based backend (EdgeEngine).

---

## The Question

When EdgeDesk and a nautilus-based backend both need the same set of indicators (e.g. Supertrend, EMA, RSI, Donchian Channel, ATR, Stochastic, VWAP, MACD, Bollinger Bands, Volume profile — roughly 5–15 total), should those indicators be:

**A) Calculated once in the backend, served to EdgeDesk via API/WebSocket**  
**B) Calculated independently in both systems**

---

## Option A: Centralized Calculation (Backend-Authoritative)

EdgeDesk fetches precomputed indicator values from the nautilus backend over WebSocket or REST. The frontend renders what the backend sends.

**Pros:**
- Single source of truth — both the UI and the execution engine see the same numbers.
- No duplication of calculation logic.
- Strategy decisions and chart overlays are guaranteed to be based on identical values.
- Easier to validate: one place to test, one place to fix.

**Cons:**
- EdgeDesk becomes dependent on the backend being live and reachable.
- Introduces latency: indicator rendering is gated on the WebSocket/REST round-trip.
- Backend serialization format must precisely encode floating-point output without precision loss.
- Nautilus's internal indicator state is opaque; extracting clean per-bar series for UI consumption requires non-trivial plumbing.
- If the backend is restarting or warming up, the chart is blank or stale until it reconnects.
- Harder to support offline/local-only analytics workflows.

---

## Option B: Dual Independent Calculation (Preferred)

EdgeDesk computes indicators locally from its own data feed. The nautilus backend computes the same indicators independently for execution decisions. Both coexist without dependency on each other.

**Pros:**
- **EdgeDesk is fully autonomous.** It can render instantly as candle data arrives, with no backend round-trip.
- **Rendering speed is decoupled from backend load.** UI stays responsive even if the backend is computing, restarting, or replaying history.
- **Simpler frontend architecture.** Candle arrives → indicator updates → chart repaints. No state synchronization problem.
- **Independent testability.** Each implementation can be validated in isolation.
- **Natural resilience.** Either side failing does not affect the other's ability to calculate.
- Suits the EdgeDesk design principle: a self-sufficient workstation that works even in standalone mode.

**Cons:**
- Redundant code: the same indicator math exists in two places (Python/JS in EdgeDesk, Rust/Python in nautilus).
- Risk of subtle numerical discrepancies between implementations (e.g. EMA seeding differences, rounding).
- Parameter changes must be applied in both places to stay consistent.
- If the execution engine acts on a slightly different indicator value than what the chart shows, debugging divergences is non-trivial.

---

## Recommendation: Option B, with a Lightweight Alignment Contract

Dual calculation is the right architectural choice for EdgeDesk given its positioning as an autonomous workstation. The frontend should never be blocked on the backend to render indicators.

However, "redundant" does not mean "unchecked." The recommended practice to keep both sides aligned:

1. **Define a canonical parameter set** for each indicator in a shared config file (e.g. `indicators.json` or a section in `config.json`). Both EdgeDesk and EdgeEngine read the same source of truth for period, multiplier, type, and timeframes.

2. **Establish a reference test suite** with a small set of frozen OHLCV sequences and their expected indicator output. Both implementations should pass the same test vectors. This makes divergences detectable before they matter in live trading.

3. **Optional divergence monitor** (future): A lightweight background task that periodically compares the backend's indicator state for the active symbol/timeframe with EdgeDesk's local values and flags differences above a small epsilon threshold.

This approach gives EdgeDesk its speed and autonomy, while providing a contractual guarantee that both sides are producing the same results.

---

## Indicator Scope (Suggested Stable Set)

| Indicator        | Category       | Notes                                      |
|------------------|----------------|--------------------------------------------|
| EMA (9, 21, 50)  | Trend          | Three periods make a readable MA ribbon    |
| Supertrend       | Trend/Signal   | ATR-based; flip events drive execution     |
| ATR              | Volatility     | Shared dependency for Supertrend and stops |
| Donchian Channel | Volatility     | Clean breakout/range signal                |
| RSI              | Momentum       | Classic; single period                     |
| Stochastic       | Momentum       | %K/%D; avoids oscillator blind spots       |
| MACD             | Momentum       | Signal line + histogram                    |
| Bollinger Bands  | Volatility     | Mean-reversion context                     |
| VWAP             | Volume/Price   | Intraday sessions only                     |
| Volume (OBV/raw) | Volume         | Trend confirmation                         |

This is 10 indicators (some multi-line). Stable enough to avoid constant churn, rich enough to cover trend, momentum, and volatility context on any timeframe.

---

## Summary

| Dimension              | Option A (Centralized) | Option B (Dual, Preferred) |
|------------------------|------------------------|----------------------------|
| Frontend autonomy      | No                     | Yes                        |
| Rendering latency      | Backend-gated          | Instant                    |
| Code duplication       | None                   | Yes (manageable)           |
| Numerical consistency  | Guaranteed             | Requires alignment contract|
| Resilience             | Coupled                | Independent                |
| Offline use            | No                     | Yes                        |
| Complexity             | Backend: higher        | Frontend: slightly higher  |

Dual calculation wins for EdgeDesk. The small cost of maintaining two implementations is outweighed by the autonomy, speed, and resilience it delivers for a workstation-grade tool.
