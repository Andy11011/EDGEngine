# Integration Map – EDGETrader Main Flow Refactoring

**Goal**: Transform the current main flow (`EDGETrader.py`) from a **single-strategy, blocking, build-once-run-once node** (MainSD1) into the target **long-running node + Postgres-driven, one-strategy-per-trade** architecture (MainSD2) – **without ever breaking the live/deployed trader**.

**Current state (MainSD1)**:

- `main()` loads creds, builds `TradingNode` once, instantiates exactly **one** `BlueprintStrategy`, calls the synchronous `run_node()` (which calls `node.run()` and blocks until `KeyboardInterrupt`), then `node.stop()` / `node.dispose()`.
- No Postgres. No dynamic strategy lifecycle. No state machine. No per-trade isolation.

**Target state (MainSD2)**:

- `TradingNode` is still built **once** and stays running via `node.run_async()`.
- A **Postgres listener** (`LISTEN trade_events` + polling fallback) runs concurrently.
- Every new trade event is **atomically claimed** (`UPDATE ... WHERE status='pending' RETURNING *`) to de-dupe the NOTIFY path vs. the poll path.
- Each claimed trade spins up its **own** `TradeStrategy` instance (unique `order_id_tag`), owning an `OrderManagementSM` (`Idle → Validating → PlacingEntry → AwaitingFill → Protecting → InPosition → Closing → Idle`).
- Strategies are added/started/stopped/removed dynamically — **many run concurrently inside the one node**.

**Strategy**:

- **Add before removing** – the existing `BlueprintStrategy` / sync `run_node()` path stays live until the new path is proven.
- **Feature flag** – a single env var (`TRADE_SOURCE_MODE`) switches between old and new behaviour per deploy.
- **Each step is a separate, deployable commit** – pausable after any step.

---

## Step 1 – Async Runner Alongside the Sync One (Additive)

**Files**: `EDGETrader.py`

**Changes**:

1. Add a new coroutine `run_node_async(node) -> None` that does `await node.run_async()` inside a `try/finally` that still calls `node.stop()` / `node.dispose()` on exit (mirrors the existing `run_node()` teardown).
2. **Leave** `run_node()` (sync) completely untouched — it's still what `main()` calls.
3. Do not call `run_node_async` from anywhere yet.

**Why this is safe**: purely additive; nothing in `main()` changes, so behavior is identical to today.

---

## Step 2 – Postgres Connectivity Module (Silent Internal)

**Files**: new `trade_events_db.py` (or a new section in `EDGETrader.py`)

**Changes**:

1. Add `psycopg` (or `asyncpg`) as a dependency.
2. Add env vars: `DATABASE_URL`, `TRADE_EVENTS_POLL_SECONDS` (default e.g. `5`).
3. Implement `async def get_pg_connection() -> Connection` and `async def claim_pending_trade(conn) -> Optional[dict]` (the atomic `UPDATE ... RETURNING *`).
4. **Do not** wire this into `main()` yet — validate it stand-alone (e.g. a small script or unit test that connects, inserts a dummy row, claims it, confirms `status='processing'`).

**Why this is safe**: no existing code path calls this module; the live trader is untouched.

---

## Step 3 – `OrderManagementSM` and `TradeStrategy` (Additive, Unused)

**Files**: `EDGETrader.py`

**Changes**:

1. Implement `OrderManagementSM` with the target states (`Idle, Validating, PlacingEntry, AwaitingFill, Protecting, InPosition, Closing`) and its transition methods, unit-testable in isolation (no Nautilus dependency needed for the SM logic itself).
2. Add `TradeStrategyConfig(StrategyConfig, frozen=True)` with `instrument_id`, `bar_type`, `order_id_tag` (mirrors `BlueprintConfig` but adds the per-trade tag).
3. Add `TradeStrategy(Strategy)`:
   - `__init__` creates `self.sm = OrderManagementSM()`.
   - `on_start()` — feeds the initial "OpenTradeEvent" into the SM, submits the entry order.
   - `on_order_filled` / `on_order_rejected` / `on_order_canceled` — feed the SM, drive `AwaitingFill → Protecting → InPosition → Closing → Idle`.
4. **Do not** instantiate `TradeStrategy` from `main()` yet. `BlueprintStrategy` remains the only strategy actually run.

**Why this is safe**: new classes sit next to the old ones; nothing references them from the live path.

---

## Step 4 – Standalone Trade-Event Listener Coroutine (Additive, Unused)

**Files**: `EDGETrader.py` (or `trade_events_db.py`)

**Changes**:

1. Implement `async def listen_trade_events(node: TradingNode, conn) -> None`:
   - `LISTEN trade_events`.
   - Inner loop: on `NOTIFY` → `claim_pending_trade`; also a periodic poll (every `TRADE_EVENTS_POLL_SECONDS`) that runs the same atomic claim as a fallback (catches anything missed during a reconnect window).
   - On a successful claim: build `TradeStrategyConfig` (`order_id_tag=trade_id`), instantiate `TradeStrategy`, `node.trader.add_strategy(strategy)`, `node.trader.start_strategy(strategy)`.
   - On trade close (SM reaches terminal `Idle`): `UPDATE trade_events SET status='closed'`, then `node.trader.stop_strategy(strategy)` / `remove_strategy(strategy)`.
2. **Do not** call this coroutine from `main()` yet. Validate it against a **test** Postgres table + a `node` running in `VIRTUAL` `TRADING_MODE`, off to the side (e.g. a manual script), confirming strategies come up/down correctly for a few synthetic trade rows.

**Why this is safe**: this is the single riskiest new piece of logic — isolating it behind "not yet called from `main()`" lets it be hammered on in isolation before it touches the real run path.

---

## Step 5 – Feature Flag Wiring in `main()` (Dual-Path)

**Files**: `EDGETrader.py`

**Changes**:

1. Add `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "SINGLE").upper()` (`SINGLE` | `EVENT_DRIVEN`), validated like `TRADING_MODE` is today.
2. In `main()`, after `node` is built (and data/exec factories registered, `node.build()` called — same as today):
   - If `trade_source_mode == "SINGLE"`: **unchanged** — instantiate `BlueprintStrategy`, call the existing `run_node(node, strategy, ...)`.
   - If `trade_source_mode == "EVENT_DRIVEN"`: instantiate a Postgres connection (Step 2), and run `node.run_async()` and `listen_trade_events(node, conn)` **concurrently** (e.g. `asyncio.gather`), inside the async teardown pattern from Step 1.
3. Default stays `SINGLE`, so **nothing changes for existing deployments** unless the env var is explicitly set.

**Why this is safe**: the branch is additive and off by default; existing deploys keep running `BlueprintStrategy` exactly as before.

---

## Step 6 – Validate `EVENT_DRIVEN` Mode End-to-End (Staging)

**Files**: none (deployment/config only)

**Changes**:

1. Deploy with `TRADE_SOURCE_MODE=EVENT_DRIVEN` and `TRADING_MODE=VIRTUAL` in a staging environment against the real (or staging) Postgres `trade_events` table.
2. Insert real trade rows and confirm:
   - NOTIFY-triggered claims work.
   - Poll fallback also claims rows if NOTIFY is (temporarily) disabled.
   - Two near-simultaneous rows don't get double-claimed (row-level atomicity).
   - Each trade gets its own `TradeStrategy`/`order_id_tag`, and multiple trades run concurrently without interfering.
   - SM reaches `Closing → Idle` correctly and the strategy is removed (`order_id_tag` freed for reuse).
3. `SINGLE` mode in production is untouched throughout.

**Why this is safe**: purely a validation step; production traffic still runs the old path.

---

## Step 7 – Promote `EVENT_DRIVEN` to Default (Soft Switch)

**Files**: `EDGETrader.py`

**Changes**:

1. Flip the default: `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "EVENT_DRIVEN").upper()`.
2. **Keep** the `SINGLE` branch and `BlueprintStrategy` fully intact — it's now the explicit opt-out (`TRADE_SOURCE_MODE=SINGLE`) rather than the default.
3. Roll out to production behind normal deploy/monitoring practice.

**Why this is safe**: any regression can be instantly reverted by setting `TRADE_SOURCE_MODE=SINGLE` — no code change needed to roll back.

---

## Step 8 – Remove the Legacy Single-Strategy Path (The Big Cleanup)

**Files**: `EDGETrader.py`

**Changes**:

1. Once `EVENT_DRIVEN` has run stably in production for a full cycle of monitoring:
   - Delete `BlueprintConfig`, `BlueprintStrategy`.
   - Delete the sync `run_node()` (superseded by `run_node_async` / the gather-based runner).
   - Delete `TRADE_SOURCE_MODE` and the `SINGLE` branch in `main()`.
   - `main()` now unconditionally: builds the node, registers factories, `node.build()`, then runs `node.run_async()` concurrently with `listen_trade_events(node, conn)`.
2. Update/remove the single-instrument `bar_type` construction in `main()` if it's no longer needed globally (each `TradeStrategy` may derive its own `bar_type` from the claimed trade payload instead).

**Why this is safe**: by this point the event-driven path has been the *only* thing production traffic exercises for a full validated cycle (Step 7); the legacy code is provably dead.

---

## Rollback / Validation at Each Step

| Step | How to validate | How to rollback |
| ------ | ------------------ | ------------------ |
| 1 | Import/run unit test on `run_node_async` in isolation; live trader unaffected. | No rollback needed (additive, unused). |
| 2 | Stand-alone script: insert dummy row, claim it, confirm status flips. | No rollback needed (internal, unused). |
| 3 | Unit-test `OrderManagementSM` transitions directly. | No rollback needed (additive, unused). |
| 4 | Run `listen_trade_events` manually against test Postgres + VIRTUAL node; confirm strategy add/remove. | No rollback needed (additive, unused). |
| 5 | Deploy with default `TRADE_SOURCE_MODE` unset — confirm behavior is byte-for-byte identical to pre-Step-5. | Ensure `TRADE_SOURCE_MODE` env var is unset/`SINGLE`. |
| 6 | Staging soak test with real trade rows, concurrent trades, NOTIFY+poll race. | Leave production on `SINGLE` (already true). |
| 7 | Production soak with `EVENT_DRIVEN` as default; monitor claim races, SM transitions, strategy cleanup. | Set `TRADE_SOURCE_MODE=SINGLE` — instant revert, no deploy needed. |
| 8 | Full app runs on event-driven path only, legacy code removed, all trades still process correctly. | Restore backed-up pre-Step-8 file. |

---

## Notes / Open Questions to Resolve Before Step 4

- **Bar subscription per trade**: MainSD1 subscribes bars once for a single hardcoded `instrument_id`/`bar_type`. MainSD2 implies each `TradeStrategy` may need its own instrument/bar subscription driven by the claimed trade's payload (`instrument`, `side`, `size`, `sl/tp`) — confirm whether `instrument_provider_config` needs to load multiple instruments up front, or lazily per trade.
- **Reconnection semantics**: confirm what "fallback poll" should query — likely `status='pending'` rows older than some grace period, to avoid re-claiming rows still legitimately in-flight from a very recent NOTIFY.
- **Credentials loading**: unchanged by this migration — `load_credentials_from_aws` / `load_ed25519_credentials_from_aws` stay exactly as in Step "Startup" of MainSD2 (including the VIRTUAL-mode skip of Ed25519 loading).
