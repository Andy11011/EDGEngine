# EdgeDesk Frontend Options for live_webhook_strategy.py

Date: 2026-04-12  
Context: `live_webhook_strategy.py` fires JSON webhook POST requests on each EMA-crossover
signal. A frontend needs to receive those signals (or poll for them), fetch or stream OHLCV
bar data independently, and render charts + signal overlays.

---

## Architecture Constraint

The strategy does not serve any HTTP endpoints itself. A minimal backend shim is always
required between the strategy and the browser (or terminal) to:

1. Accept the inbound webhook POST
2. Persist or buffer signals (in memory, SQLite, Redis, etc.)
3. Serve them to the frontend (REST poll, WebSocket push, or SSE stream)

This shim can be as small as ~50 lines of FastAPI/Flask.

---

## Option 1 — Python FastAPI + TradingView Lightweight Charts (recommended)

**What it is:**  
TradingView Lightweight Charts is the open-source, Apache-2.0 charting library that
powers TradingView embeds. It is pure JavaScript, under 50 KB gzipped, and renders
professional OHLCV candlestick charts with marker overlays.

**Stack:**

- `FastAPI` or `Flask` as the webhook receiver and static file server
- WebSocket or Server-Sent Events (SSE) to push signals from server to browser
- `lightweight-charts` JS library in a single HTML file for the chart canvas
- Binance public REST/WebSocket for live OHLCV bar data (no auth required)

**Signal flow:**

```
NautilusTrader           FastAPI shim           Browser
live_webhook_strategy ──► POST /signal ──► SSE/WS ──► lightweight-charts marker
Binance public WS ────────────────────────────────────► candlestick series
```

**Pros:**

- Lightweight Charts renders exactly like TradingView — professional quality
- Zero licensing cost, Apache 2.0
- No Java, no build toolchain — a single HTML file + 100-line Python server
- Works in Docker alongside the strategy container
- Signal markers (`createSeriesMarker`) overlay directly on bars

**Cons:**

- No built-in replay/backtest view (can be added manually)
- Binance bar fetch is a separate concern from the strategy

**Minimal server — ~60 lines:**

```python
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse
import asyncio, json, collections

app = FastAPI()
_signals: collections.deque = collections.deque(maxlen=500)
_subscribers: list[asyncio.Queue] = []

@app.post("/signal")
async def receive_signal(payload: dict):
    _signals.append(payload)
    for q in _subscribers:
        await q.put(payload)
    return {"ok": True}

@app.get("/stream")
async def stream(request):
    q = asyncio.Queue()
    _subscribers.append(q)
    async def gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                data = await q.get()
                yield {"data": json.dumps(data)}
        finally:
            _subscribers.remove(q)
    return EventSourceResponse(gen())
```

**Repo:** <https://github.com/tradingview/lightweight-charts>

---

## Option 2 — Apache ECharts (pure JS, no Java required)

**What it is:**  
ECharts is a full-featured charting library from Apache (Apache 2.0). It has a native
candlestick chart type and supports custom mark-points / overlays for signals.

**Java is not required.** Java would only be used as a backend language (e.g. Spring Boot),
which adds significant complexity with no benefit over a Python or Node.js shim. ECharts
itself runs entirely in the browser.

**Stack:**

- Same FastAPI/Flask webhook shim as Option 1
- ECharts 5.x in the browser for candlestick rendering

**Pros:**

- More chart types than Lightweight Charts (heatmaps, scatter plots, bar histograms)
- Better for building multi-panel analytics dashboards
- Large ecosystem and documentation

**Cons:**

- Heavier (~1 MB bundle) vs Lightweight Charts (~50 KB)
- More configuration boilerplate for financial charts specifically
- TradingView Lightweight Charts looks more "native" for candlestick trading UIs

**Repo:** <https://github.com/apache/echarts>

---

## Option 3 — Grafana + InfluxDB (ops/monitoring style)

**What it is:**  
The webhook shim writes signals into InfluxDB (or Prometheus). Grafana reads from it and
renders a time-series dashboard with annotations for signal events.

**Stack:**

- FastAPI webhook shim writing to InfluxDB Line Protocol
- InfluxDB OSS v2 for storage
- Grafana OSS with the `grafana-fin-charts-panel` or `candlestick` panel plugin

**Pros:**

- Zero custom frontend code
- Production-grade alerting, history, and annotation support
- Docker Compose makes the whole stack deployable in one file

**Cons:**

- No real-time candlestick chart that looks like a trading terminal
- Grafana's financial chart plugins are limited compared to Lightweight Charts
- Overhead of running InfluxDB + Grafana for what is essentially a signal viewer

**Best for:** teams already running Grafana infrastructure who want signal history and
alerting without building any UI.

---

## Option 4 — Streamlit (fastest prototype, Python-native)

**What it is:**  
Streamlit turns a Python script into a web app with zero HTML/JS. `plotly.graph_objects`
provides candlestick charts.

**Stack:**

- Streamlit app polls a SQLite or Redis-backed signal store
- `plotly` for candlestick + scatter signal overlay

**Pros:**

- Pure Python — no JS, HTML, or backend server to write
- `st.rerun()` with a short `time.sleep` gives near-real-time refresh
- Signal table + chart in ~80 lines of Python

**Cons:**

- Page-reload model is not truly real-time (polling only)
- Not suitable for a production terminal — more of a dev/monitoring tool
- Plotly candlestick rendering is heavier than Lightweight Charts

**Repo:** <https://github.com/streamlit/streamlit>

---

## Option 5 — Terminal UI (no browser required)

For a signal monitor that runs in the same SSH session as the strategy:

### 5a. Textual (Python TUI framework)

- Full-featured TUI with widgets, tables, layout, and reactive updates
- Can display a scrolling signal log, EMA values, and last-bar info in panels
- Does not render candlestick charts natively, but can render sparklines
- **Repo:** <https://github.com/Textualize/textual>

### 5b. Rich (Python terminal output)

- Simpler than Textual; useful for colorized signal tables piped from the webhook
- Not interactive
- **Repo:** <https://github.com/Textualize/rich>

### 5c. plotext (Python terminal charts)

- Renders ASCII/Unicode line charts and bar charts inside a terminal
- Can plot EMA values and mark signal crossover points on a line chart
- No candlestick support
- **Repo:** <https://github.com/piccolomo/plotext>

### 5d. Grafterm (Go, terminal Grafana)

- Terminal dashboard that queries time-series backends (Prometheus, Graphite)
- Renders line charts in Unicode inside a terminal
- Requires Prometheus or Graphite as the data source
- **Repo:** <https://github.com/slok/grafterm>

---

---

## Option 6 — Complete Open-Source Trading Frontend Projects

These are full applications (not just libraries) that provide a ready-made chart terminal
experience. Think of them as "Nautilus but for the UI layer."

---

### 6a. OpenBB Platform / OpenBB Terminal (6k commits)

**The most direct equivalent to Nautilus, but for the frontend.**

OpenBB is a full open-source financial research terminal. It started as a CLI (OpenBB
Terminal) and has evolved into a Python platform with a browser-based UI. The open-source
core is MIT/AGPLv3 licensed.

- Full candlestick charts, technical indicators, overlays
- Data provider abstraction (Binance, Polygon, yFinance, etc.)
- Extensible: custom indicators and data widgets via Python extensions
- REST API backend that can be pointed at custom data sources
- Closest analogue to "what EdgeDesk wants to be" in the open-source world

**Repo:** <https://github.com/OpenBB-finance/OpenBB>  
**Language:** Python backend, React/TypeScript frontend  
**License:** AGPLv3 (core), MIT (OpenBB Platform)

---

### 6b. Superalgos (23k commits)

**Visual strategy designer + live chart terminal, all-in-one.**

Superalgos is a Node.js application that runs a full visual trading platform:

- Interactive chart timeline with indicator overlays and strategy execution nodes
- Built-in backtesting and live execution visualization
- Connects to Binance and other exchanges natively
- Charts are built on a custom WebGL canvas renderer

Architecture is very different from Nautilus — everything is JSON-configurable visual
nodes rather than Python code. Good reference for what a production trading chart
terminal looks like end-to-end.

**Repo:** <https://github.com/Superalgos/Superalgos>  
**Language:** JavaScript (Node.js)  
**License:** Apache 2.0

---

### 6c. FreqUI (Freqtrade's official web UI)

**Complete live-trading dashboard with charts, signal history, and P&L.**

FreqUI is the official web frontend for the Freqtrade crypto trading bot. It is a
standalone Vue.js application that connects to the Freqtrade REST API.

- Live candlestick charts via **TradingView Lightweight Charts**
- Trade history with entry/exit signal markers overlaid on bars
- Real-time profit tables, drawdown charts, balance history
- Strategy comparison view
- Can be self-hosted without Freqtrade if the REST API is reimplemented

This is probably the fastest path to a working signal-overlay chart dashboard because
the entire "chart + signal markers" backend/frontend pattern is already built.
Forking and replacing Freqtrade's API calls with EdgeDesk's webhook/signal API is a
bounded, well-defined task.

**Repo:** <https://github.com/freqtrade/frequi>  
**Language:** Vue 3 + TypeScript  
**License:** GPL v3

---

### 6d. Jesse Dashboard

**Clean web UI for the Jesse Python trading framework.**

Jesse is a Python trading framework (similar scope to Nautilus). Its companion web UI:

- Strategy list, live results, and import/run controls
- Equity curve and trade-level charts (Plotly-based)
- Backtesting results viewer

Lighter than OpenBB or Superalgos. Good reference implementation for the pattern of
"Python trading engine + decoupled React/Vue dashboard."

**Repo:** <https://github.com/jesse-ai/jesse-ui>  
**Language:** Vue 2 + JavaScript  
**License:** MIT

---

### 6e. KLineChart

**The most full-featured open-source candlestick chart component.**

If TradingView Lightweight Charts is "the standard" candlestick library, KLineChart is
the feature-complete alternative:

- Full indicator suite (MACD, RSI, Bollinger, EMA, etc.) built-in per pane
- Custom overlay drawing tools (trend lines, Fibonacci, etc.)
- Cross-hair, zoom, pan, TypeScript, 0 external dependencies
- Comparable bundle size to Lightweight Charts (~80 KB gzipped)

Strongest drop-in if Lightweight Charts' API proves limiting.

**Repo:** <https://github.com/liihuu/KLineChart>  
**Language:** TypeScript  
**License:** Apache 2.0

---

### 6f. react-financial-charts

**React component library with full D3-powered financial chart primitives.**

A fork of the original `react-stockcharts`:

- Candlestick, OHLC, area, renko, point-and-figure series
- Indicator overlays (EMA, Bollinger, VWAP, etc.)
- Annotation layers — ideal for signal event markers
- Each chart element is a React component, making layout fully composable

**Repo:** <https://github.com/reactivemarkets/react-financial-charts>  
**Language:** React + TypeScript  
**License:** MIT

---

### Summary of Complete Projects

| Project                   | Type                 | Language        | Signal overlay  | Live data | Effort to adapt |
|---------------------------|----------------------|-----------------|-----------------|-----------|-----------------|
| OpenBB Platform           | Full terminal        | Python + React  | Custom widget   | Yes       | Medium          |
| Superalgos                | Full visual platform | Node.js         | Native          | Yes       | High            |
| FreqUI                    | Trading dashboard    | Vue 3 + TS      | Native          | Yes       | Low (fork it)   |
| Jesse Dashboard           | Backtest/live UI     | Vue 2 + JS      | Via Plotly      | Partial   | Low             |
| KLineChart (component)    | Chart component      | TypeScript      | Custom overlay  | Yes       | Low             |
| react-financial-charts    | Chart component      | React + TS      | Annotation layer| Yes       | Low             |

**For EdgeDesk:** FreqUI is the most immediately reusable — it already implements exactly
the pattern needed (Python trading engine → REST → Vue frontend with Lightweight Charts
- signal markers). Forking it and replacing Freqtrade API calls with EdgeDesk's
webhook/signal API is a well-defined, bounded task.

---

## Comparison Table

| Option                        | Chart quality | Real-time | Effort | Language  | Notes                          |
|-------------------------------|---------------|-----------|--------|-----------|--------------------------------|
| FastAPI + Lightweight Charts  | ★★★★★         | ★★★★★     | Low    | Python+JS | Best overall for EdgeDesk      |
| FastAPI + ECharts             | ★★★★☆         | ★★★★★     | Low    | Python+JS | Better for analytics panels    |
| Grafana + InfluxDB            | ★★★☆☆         | ★★★★☆     | Medium | Any       | Good if infra already exists   |
| Streamlit + Plotly            | ★★★☆☆         | ★★★☆☆     | Very low | Python  | Best for quick prototyping     |
| Textual (terminal)            | ★★☆☆☆         | ★★★★☆     | Low    | Python    | Good for SSH/headless server   |
| plotext (terminal)            | ★★☆☆☆         | ★★★☆☆     | Very low | Python  | Minimal — no candlesticks      |

---

## Recommendation for EdgeDesk

**Phase 1 — quickest working UI:**  
Streamlit + Plotly. Add a SQLite-backed signal store, poll every 2 seconds. Usable in
under a day.

**Phase 2 — production terminal UI:**  
FastAPI shim + TradingView Lightweight Charts. This matches EdgeDesk's positioning as
a "terminal-style interface" and requires no Java, no build tools, and no heavy framework.
The chart looks and behaves exactly like a professional trading terminal.

**If Java is a hard requirement:**  
Use Spring Boot as the webhook receiver and serve a React + ECharts frontend. This is a
valid enterprise stack but adds significant boilerplate compared to the Python path. There
is no technical advantage over FastAPI + Lightweight Charts for this use case.

---

## Note on Java

Java is a viable backend language but adds no benefit here over Python, since:

- The strategy itself is Python
- The shim is trivially small (webhook receiver + SSE broadcaster)
- ECharts and Lightweight Charts both run in the browser regardless of backend language

The only reason to choose Java would be if the team already has Java infrastructure or
plans to grow the backend into a larger JVM-based service.
