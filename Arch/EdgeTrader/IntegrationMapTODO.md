## EDGETrader Main Flow Refactoring (Integration Map)

### Step 1 – Async Runner Alongside the Sync One (Additive)

- [X] ~~*Add coroutine `run_node_async(node) -> None` (`await node.run_async()` in `try/finally`, same teardown as `run_node()`)*~~ [2026-08-01]
- [X] ~~*Leave `run_node()` (sync) completely untouched — still called by `main()`*~~ [2026-08-01]
- [X] ~~*Do not call `run_node_async` from anywhere yet*~~ [2026-08-01]

### Step 2 – Postgres Connectivity Module (Silent Internal)

- [ ] Add `psycopg` (or `asyncpg`) dependency
- [ ] Add env vars `DATABASE_URL`, `TRADE_EVENTS_POLL_SECONDS` (default `5`)
- [ ] Implement `async def get_pg_connection() -> Connection`
- [ ] Implement `async def claim_pending_trade(conn) -> Optional[dict]` (atomic `UPDATE ... RETURNING *`)
- [ ] Validate stand-alone (insert dummy row, claim it, confirm `status='processing'`) — do **not** wire into `main()` yet

### Step 3 – `OrderManagementSM` and `TradeStrategy` (Additive, Unused)

- [ ] Implement `OrderManagementSM` with states `Idle, Validating, PlacingEntry, AwaitingFill, Protecting, InPosition, Closing` + transitions
- [ ] Unit-test `OrderManagementSM` transitions in isolation (no Nautilus dependency needed)
- [ ] Add `TradeStrategyConfig(StrategyConfig, frozen=True)` with `instrument_id`, `bar_type`, `order_id_tag`
- [ ] Add `TradeStrategy(Strategy)`:
  - [ ] `__init__` creates `self.sm = OrderManagementSM()`
  - [ ] `on_start()` feeds initial "OpenTradeEvent" into SM, submits entry order
  - [ ] `on_order_filled` / `on_order_rejected` / `on_order_canceled` feed SM, drive `AwaitingFill → Protecting → InPosition → Closing → Idle`
- [ ] Do not instantiate `TradeStrategy` from `main()` yet — `BlueprintStrategy` remains the only strategy actually run

### Step 4 – Standalone Trade-Event Listener Coroutine (Additive, Unused)

- [ ] Implement `async def listen_trade_events(node, conn) -> None`:
  - [ ] `LISTEN trade_events`
  - [ ] On `NOTIFY` → `claim_pending_trade`
  - [ ] Periodic poll fallback (every `TRADE_EVENTS_POLL_SECONDS`) using the same atomic claim
  - [ ] On successful claim: build `TradeStrategyConfig` (`order_id_tag=trade_id`), instantiate `TradeStrategy`, `add_strategy` + `start_strategy`
  - [ ] On trade close (SM reaches terminal `Idle`): `UPDATE trade_events SET status='closed'`, then `stop_strategy` + `remove_strategy`
- [ ] Validate against test Postgres table + `VIRTUAL` `TRADING_MODE` node, off to the side (manual script), for a few synthetic trade rows
- [ ] Do not call this coroutine from `main()` yet

### Step 5 – Feature Flag Wiring in `main()` (Dual-Path)

- [ ] Add `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "SINGLE").upper()` (`SINGLE` | `EVENT_DRIVEN`), validated like `TRADING_MODE`
- [ ] `SINGLE` branch: unchanged — instantiate `BlueprintStrategy`, call existing `run_node(...)`
- [ ] `EVENT_DRIVEN` branch: open Postgres connection (Step 2), run `node.run_async()` and `listen_trade_events(node, conn)` concurrently (`asyncio.gather`) inside async teardown (Step 1)
- [ ] Confirm default stays `SINGLE` — no behavior change for existing deployments

### Step 6 – Validate `EVENT_DRIVEN` Mode End-to-End (Staging)

- [ ] Deploy with `TRADE_SOURCE_MODE=EVENT_DRIVEN` + `TRADING_MODE=VIRTUAL` in staging against real/staging `trade_events` table
- [ ] Confirm NOTIFY-triggered claims work
- [ ] Confirm poll fallback claims rows if NOTIFY is temporarily disabled
- [ ] Confirm two near-simultaneous rows don't get double-claimed (row-level atomicity)
- [ ] Confirm each trade gets its own `TradeStrategy`/`order_id_tag`, multiple trades run concurrently without interfering
- [ ] Confirm SM reaches `Closing → Idle` correctly and strategy is removed (`order_id_tag` freed for reuse)
- [ ] Confirm `SINGLE` mode in production remains untouched throughout

### Step 7 – Promote `EVENT_DRIVEN` to Default (Soft Switch)

- [ ] Flip default: `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "EVENT_DRIVEN").upper()`
- [ ] Keep `SINGLE` branch and `BlueprintStrategy` fully intact as explicit opt-out
- [ ] Roll out to production under normal deploy/monitoring practice

### Step 8 – Remove the Legacy Single-Strategy Path (The Big Cleanup)

- [ ] Confirm `EVENT_DRIVEN` has run stably in production for a full monitoring cycle
- [ ] Delete `BlueprintConfig`, `BlueprintStrategy`
- [ ] Delete sync `run_node()`
- [ ] Delete `TRADE_SOURCE_MODE` and the `SINGLE` branch in `main()`
- [ ] `main()` unconditionally: build node → register factories → `node.build()` → `node.run_async()` concurrently with `listen_trade_events(node, conn)`
- [ ] Update/remove single-instrument `bar_type` construction in `main()` if no longer needed globally (each `TradeStrategy` derives its own from the claimed trade payload)

### Open Questions (resolve before Step 4)

- [ ] Confirm bar/instrument subscription model per trade (upfront multi-instrument load vs. lazy per-trade)
- [ ] Confirm fallback-poll query semantics (grace period to avoid re-claiming rows still in-flight from a recent NOTIFY)
- [ ] Confirm credentials loading (`load_credentials_from_aws` / `load_ed25519_credentials_from_aws`) stays unchanged, including VIRTUAL-mode Ed25519 skip
