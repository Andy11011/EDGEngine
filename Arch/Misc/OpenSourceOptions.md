Open-Source Base Options for a TradingView-Like Terminal

Date: 2026-04-04
Context: Goal is to avoid building the full stack from scratch while still getting:

- strong chart/UI terminal experience
- backtesting support
- permutation / parameter testing support
- lighter implementation effort than building an all-in-one custom platform from zero

Problem Framing

The main issue with EDGEngine is not raw size alone. The issue is implementation scope. Building all of the following from scratch is a large project:

- chart terminal UI
- indicator system
- live data pipeline
- backtesting engine
- optimization / permutation testing
- brokerage/exchange adapters
- result storage and visualization

The best practical strategy is to start from an existing engine or research framework and build only the missing product layer.

Bottom-Line Recommendation

There is no single open-source project that cleanly gives all three of these at production quality:

1. TradingView-like UI terminal
2. strong backtesting engine
3. strong permutation testing / parameter surface exploration

The realistic approach is:

- choose an open-source backend base
- pair it with a custom frontend terminal
- keep optimization/permutation testing as a separate service or module

Best Options

1. vectorbt + custom frontend

Best fit when the main priorities are:

- fast research iteration
- parameter sweeps
- permutation testing
- statistical analysis
- lighter architecture

Strengths:

- excellent for vectorized backtesting and large parameter sweeps
- very strong for sensitivity analysis and optimization workflows
- much lighter than a full event-driven trading platform
- good fit for building a modern custom UI on top

Weaknesses:

- live trading and brokerage connectivity are not its main strength
- not a ready-made trading terminal
- event-driven execution realism is weaker than dedicated trading engines

Best use:

- build a research-first terminal
- add live execution later through a separate execution adapter

Assessment:

- UI terminal: custom build required
- backtesting: yes
- permutation testing: very good
- live trading base: moderate
- implementation effort: relatively low to moderate

1. NautilusTrader + custom frontend

Best fit when the main priorities are:

- serious trading architecture
- event-driven design
- cleaner live/backtest engine foundation
- modern engineering base

Strengths:

- stronger engine architecture than many older Python stacks
- good fit for serious live/backtest workflows
- better long-term systems base if execution realism matters

Weaknesses:

- no polished built-in TradingView-like terminal UI
- permutation testing is possible, but not the main built-in differentiator
- still requires substantial product-layer work

Best use:

- build a proper terminal product over a modern engine core

Assessment:

- UI terminal: custom build required
- backtesting: yes
- permutation testing: good, but more custom work
- live trading base: strong
- implementation effort: moderate

1. Lean + custom frontend

Best fit when the main priorities are:

- mature multi-asset engine
- broad brokerage/exchange support
- established backtesting/live/research workflows

Strengths:

- very capable engine
- broad asset and brokerage support
- strong backtesting and research ecosystem
- good long-term engine foundation

Weaknesses:

- no built-in product-grade terminal UI
- charting exists mainly as result/chart data structures, not as a full chart workstation
- permutation testing is possible, but not the cleanest base if this is the central feature

Best use:

- if engine maturity and breadth matter more than implementation simplicity

Assessment:

- UI terminal: custom build required
- backtesting: yes
- permutation testing: moderate to good
- live trading base: strong
- implementation effort: moderate to high

1. Freqtrade + custom dashboard

Best fit when the main priorities are:

- crypto trading only
- getting started faster
- acceptable compromises on architecture breadth

Strengths:

- easier path for crypto-only workflows
- useful ecosystem around strategy testing and optimization
- lighter than building a full custom platform

Weaknesses:

- narrower domain than Lean or NautilusTrader
- UI is functional, not TradingView-like
- not ideal if the long-term goal is a broader trading terminal platform

Best use:

- quick crypto-focused trading product

Assessment:

- UI terminal: custom build required for premium UX
- backtesting: yes
- permutation testing: decent
- live trading base: good for crypto
- implementation effort: relatively low

1. Backtrader or QSTrader + custom frontend

Best fit when the main priorities are:

- simple backtesting base
- lower conceptual complexity

Strengths:

- simple mental model
- easy to prototype against

Weaknesses:

- older feel
- not ideal as the main foundation for a modern terminal product
- likely to be outgrown if UI + optimization + live integration become serious requirements

Assessment:

- UI terminal: custom build required
- backtesting: yes
- permutation testing: possible but mostly custom
- live trading base: limited compared with stronger engines
- implementation effort: low initially, but higher over time due to limitations

UI Layer Recommendation

If the requirement is a TradingView-like experience, the UI should be treated as its own product layer.

Best open-source UI building blocks:

1. TradingView Lightweight Charts

- best fit for candlestick charts and terminal-style feel
- important note: this is not the full TradingView Charting Library

1. Apache ECharts

- best fit for:
  - heatmaps
  - equity curves
  - drawdowns
  - parameter surfaces
  - statistical dashboards

1. React or Vue frontend shell

- React is strong if you want a broader component ecosystem
- Vue is strong if you want a simpler reactive UI layer
- either can work well with lightweight-charts and websocket updates

Recommended Architecture Patterns

Option A: Research-first lightweight stack

- Frontend: React or Vue
- Charts: Lightweight Charts
- Analytics charts: ECharts
- API: FastAPI
- Backtesting / permutation core: vectorbt
- Optional live execution: separate service later

Why this is good:

- lowest implementation burden for rich analytics + optimization
- best if permutation testing is a central product feature
- easiest path to an EDGEngine-lite product

Tradeoff:

- live trading architecture is weaker than dedicated engine-first stacks

Option B: Engine-first stack

- Frontend: React or Vue
- Charts: Lightweight Charts
- Analytics charts: ECharts
- Engine: NautilusTrader or Lean
- Optimization worker: separate permutation/grid-search service
- Storage: Postgres or lightweight result store

Why this is good:

- stronger foundation for serious live trading
- better separation between terminal, engine, and optimization

Tradeoff:

- more implementation effort than Option A

Recommended Ranking for This Project Goal

If the goal is specifically:

- TradingView-like UI
- backtesting
- permutation testing
- less from-scratch work than EDGEngine

then the best ranking is:

1. vectorbt + custom terminal UI
2. NautilusTrader + custom terminal UI
3. Lean + custom terminal UI
4. Freqtrade + custom terminal UI
5. Backtrader / QSTrader

Practical Recommendation

Recommended default choice:

vectorbt + FastAPI + Lightweight Charts + ECharts

Reason:

- it minimizes how much core quantitative infrastructure must be built from scratch
- it is strongest for permutation testing and parameter exploration
- it gives a clean path to a polished custom terminal UI
- it is lighter in implementation scope than recreating a full engine/UI stack from zero

Choose NautilusTrader instead if:

- live trading architecture matters more than permutation testing speed
- you want a stronger event-driven engine base from day one

Choose Lean instead if:

- brokerage breadth and engine maturity matter more than product simplicity
- you are willing to build more of the UI and optimization product layer yourself

What to Avoid

1. Building everything into one large monolith from the start
2. Coupling chart UI directly to runtime strategy execution
3. Expecting the open-source engine to provide the terminal product UX
4. Starting with the heaviest engine if optimization/permutation UX is the real differentiator

Suggested MVP Direction

The most pragmatic MVP is:

- custom frontend terminal
- one charting layer for candles and overlays
- one backtesting/research backend
- one optimization/permutation worker
- no full live trading stack in v1 unless absolutely needed

This gives the shortest path to a useful product without repeating the full EDGEngine build-from-scratch effort.
