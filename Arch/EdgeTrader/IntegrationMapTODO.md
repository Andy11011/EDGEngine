## EDGETrader Main Flow Refactoring (Integration Map — SNS/SQS + Lambda)

### Step 1 – Async Runner Alongside the Sync One (Additive)

- [X] ~~*Add coroutine `run_node_async(node) -> None` (`await node.run_async()` in `try/finally`, same teardown as `run_node()`)*~~ [2026-08-01]
- [X] ~~*Leave `run_node()` (sync) completely untouched — still called by `main()`*~~ [2026-08-01]
- [X] ~~*Do not call `run_node_async` from anywhere yet*~~ [2026-08-01]

### Step 2 – SQS Queue + Consumer Module (Silent Internal)

- [X] ~~*Provision `trade-events-queue` (Standard) subscribed to `trade-events` SNS topic*~~ [2026-08-03]
- [X] ~~*Provision `trade-events-queue-dlq` with redrive policy (`maxReceiveCount=5`)*~~ [2026-08-03]
- [X] ~~*Add env vars `SQS_TRADE_EVENTS_QUEUE_URL`, `SQS_POLL_WAIT_SECONDS` (default `20`), `SQS_MAX_MESSAGES` (default `10`)*~~ [2026-08-03]
- [X] ~~*Implement `async def receive_trade_events(sqs_client, queue_url) -> List[dict]` (long-poll `ReceiveMessage`)*~~ [2026-08-03]
- [X] ~~*Implement `async def delete_message(sqs_client, queue_url, receipt_handle) -> None`*~~ [2026-08-03]
- [ ] Validate stand-alone: publish via Step 2 topic, confirm this module receives + deletes the message
- [X] ~~*Do not wire into `main()` yet*~~ [2026-08-03]

### Step 3 – `OrderManagementSM` and `TradeStrategy` (Additive, Unused)

- [X] ~~*Implement `OrderManagementSM` with states `Validating, PlacingEntry, AwaitingFill, Protecting, InPosition, Canceling,Closing` + transitions*~~ [2026-08-04]
- [ ] Unit-test `OrderManagementSM` transitions in isolation (no Nautilus dependency needed)
- [X] ~~*Add `TradeStrategyConfig(StrategyConfig, frozen=True)` with `instrument_id`, `bar_type`, `order_id_tag`, deterministic `client_order_id` (derived from `trade_id`)*~~ [2026-08-05]
- [X] ~~*Add `TradeStrategy(Strategy)`:*~~ [2026-08-05]
  - [X] ~~*`__init__` creates `self.sm = OrderManagementSM()`*~~ [2026-08-05]
  - [X] ~~*`on_start()` feeds initial "OpenTradeEvent" into SM, submits entry order using the deterministic `client_order_id`*~~ [2026-08-05]
  - [X] ~~*`on_order_filled` / `on_order_rejected` / `on_order_canceled` feed SM, drive `AwaitingFill → Protecting → InPosition → Closing → Idle`*~~ [2026-08-05]
- [X] ~~*Do not instantiate `TradeStrategy` from `main()` yet — `BlueprintStrategy` remains the only strategy actually run*~~ [2026-08-05]

### Step 4 – Standalone SQS Trade-Event Listener Coroutine (Additive, Unused)

- [X] ~~*Implement `async def listen_trade_events(node, sqs_client, queue_url) -> None`:*~~ [2026-08-05]
  - [X] ~~*Long-poll loop via `receive_trade_events`*~~ [2026-08-05]
  - [X] ~~*For each message: parse `event_id`, call existing `TradeEventsDB.claim_event(event_id)` (unchanged Postgres claim)*~~ [2026-08-05]
  - [X] ~~*If claimed: build `TradeStrategyConfig` (`order_id_tag=trade_id`), instantiate `TradeStrategy`, `add_strategy` + `start_strategy`*~~ [2026-08-05]
  - [X] ~~*If not claimed (duplicate): no-op*~~ [2026-08-05]
  - [X] ~~*Delete the SQS message once claimed-and-handed-off (or recognized as duplicate) — not held open until trade close*~~ [2026-08-05]
  - [X] ~~*On trade close (SM reaches terminal `Idle`): append closing event to `trade_events`, then `stop_strategy` + `remove_strategy`*~~ [2026-08-05]
- [ ] Document the claim-then-crash gap (claimed but never started) as a known limitation; note deterministic `client_order_id` as the backstop
- [ ] Validate against test SQS queue + `VIRTUAL` `TRADING_MODE` node, off to the side (manual script): strategy add/remove + duplicate-delivery skip
- [X] ~~*Do not call this coroutine from `main()` yet*~~ [2026-08-05]

### Step 5 – Telegram Notifier Lambda (Additive, Independent Subscriber)

- [ ] Create Lambda `trade-events-telegram-notifier`, subscribed directly to `trade-events` SNS topic
- [ ] Format human-readable message from SNS payload (`event_type`, `trade_id`, `instrument`, `side`, `ep`/`fill_price`, `close_reason`, ...)
- [ ] Call Telegram Bot API `sendMessage` with bot token pulled from AWS Secrets Manager
- [ ] Confirm this path does **not** touch `processed_events` — duplicates here are acceptable
- [ ] Validate stand-alone: publish test SNS message, confirm Telegram delivery; confirm duplicate publish → duplicate (acceptable) message, not an error

### Step 6 – Feature Flag Wiring in `main()` (Dual-Path)

- [ ] Add `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "SINGLE").upper()` (`SINGLE` | `EVENT_DRIVEN`), validated like `TRADING_MODE`
- [ ] `SINGLE` branch: unchanged — instantiate `BlueprintStrategy`, call existing `run_node(...)`
- [ ] `EVENT_DRIVEN` branch: create SQS client (Step 3), run `node.run_async()` and `listen_trade_events(node, sqs_client, queue_url)` concurrently (`asyncio.gather`) inside async teardown (Step 1)
- [ ] Confirm default stays `SINGLE` — no behavior change for existing deployments

### Step 7 – Validate `EVENT_DRIVEN` Mode End-to-End (Staging)

- [ ] Deploy with `TRADE_SOURCE_MODE=EVENT_DRIVEN` + `TRADING_MODE=VIRTUAL` in staging against real/staging SNS/SQS + Postgres
- [ ] Confirm SNS → SQS delivery works; each event claimed and acted on exactly once
- [ ] Confirm SNS → Lambda → Telegram delivery works independently of the trading path
- [ ] Confirm simulated duplicate SQS deliveries are correctly skipped by `claim_event`, no duplicate orders
- [ ] Confirm forced poison messages land in the DLQ after `maxReceiveCount`, not lost or looping
- [ ] Confirm each trade gets its own `TradeStrategy`/`order_id_tag`, multiple trades run concurrently without interfering
- [ ] Confirm SM reaches `Closing → Idle` correctly and strategy is removed (`order_id_tag` freed for reuse)
- [ ] Add and test the reconciliation script (Step 2) against an injected "publish failed" case
- [ ] Confirm `SINGLE` mode in production remains untouched throughout

### Step 8 – Promote `EVENT_DRIVEN` to Default (Soft Switch)

- [ ] Flip default: `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "EVENT_DRIVEN").upper()`
- [ ] Keep `SINGLE` branch and `BlueprintStrategy` fully intact as explicit opt-out
- [ ] Set up CloudWatch alarm on DLQ depth before/at rollout
- [ ] Roll out to production under normal deploy/monitoring practice

### Step 9 – Remove the Legacy Single-Strategy Path (The Big Cleanup)

- [ ] Confirm `EVENT_DRIVEN` has run stably in production for a full monitoring cycle
- [ ] Delete `BlueprintConfig`, `BlueprintStrategy`
- [ ] Delete sync `run_node()`
- [ ] Delete `TRADE_SOURCE_MODE` and the `SINGLE` branch in `main()`
- [ ] `main()` unconditionally: build node → register factories → `node.build()` → `node.run_async()` concurrently with `listen_trade_events(node, sqs_client, queue_url)`
- [ ] Update/remove single-instrument `bar_type` construction in `main()` if no longer needed globally

### Open Questions (resolve before Step 5)

- [ ] Confirm bar/instrument subscription model per trade (upfront multi-instrument load vs. lazy per-trade)
- [ ] Confirm whether the outbox publish-failure gap needs a transactional outbox table before production, or reconciliation-after-the-fact is sufficient
- [ ] Confirm DLQ `maxReceiveCount` and alerting policy
- [ ] Confirm whether the claim-then-crash reconciliation check (Step 5) is needed before Step 9, or the deterministic `client_order_id` backstop is sufficient at current volume
- [ ] Confirm credentials loading (`load_credentials_from_aws` / `load_ed25519_credentials_from_aws`) stays unchanged, including VIRTUAL-mode Ed25519 skip
