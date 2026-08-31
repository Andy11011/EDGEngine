# Integration Map – EDGETrader Main Flow Refactoring (SNS/SQS + Lambda)

**Goal**: Transform the current main flow (`EDGETrader.py`) from a **single-strategy, blocking, build-once-run-once node** (MainSD1) into the target **long-running node + SNS/SQS-driven, one-strategy-per-trade** architecture (MainSD2) – **without ever breaking the live/deployed trader**.

**Revision note**: This supersedes the earlier Postgres `LISTEN/NOTIFY` + poll-fallback design. `trade_events` / `processed_events` in Postgres remain the durable source of truth and the idempotency guard (unchanged — see `trades_db_async.py`). What changes is the **transport**: instead of the trading node listening directly to Postgres, new rows are published to an **SNS topic**, which fans out to (a) an **SQS queue** consumed by the trading node, and (b) a **Lambda** that posts trade notifications to Telegram. This removes the NOTIFY-vs-poll race entirely (SQS has one delivery path, not two) and adds a Telegram channel for free as a second, independent subscriber.

**Current state (MainSD1)**:

- `main()` loads creds, builds `TradingNode` once, instantiates exactly **one** `BlueprintStrategy`, calls the synchronous `run_node()` (which calls `node.run()` and blocks until `KeyboardInterrupt`), then `node.stop()` / `node.dispose()`.
- No Postgres. No dynamic strategy lifecycle. No state machine. No per-trade isolation.

**Target state (MainSD2)**:

- `TradingNode` is still built **once** and stays running via `node.run_async()`.
- Trade events are written to Postgres (`trade_events`, unchanged) and **published to an SNS topic** (`trade-events`) in the same code path that writes the row (the "outbox" step).
- SNS fans out to an **SQS queue** (`trade-events-queue`, with a DLQ), long-polled by the trading node, and independently to a **Lambda** (`trade-events-telegram-notifier`) for human-readable alerts.
- Every claimed event is deduped via the existing atomic `processed_events` claim (`INSERT ... ON CONFLICT DO NOTHING RETURNING`) — unchanged from the Postgres-only design, just checked per SQS message instead of per NOTIFY/poll row.
- Each claimed trade spins up its **own** `TradeStrategy` instance (unique `order_id_tag` / deterministic Binance `client_order_id`), owning an `OrderManagementSM` (`Idle → Validating → PlacingEntry → AwaitingFill → Protecting → InPosition → Closing → Idle`).
- Strategies are added/started/stopped/removed dynamically — **many run concurrently inside the one node**.

**Strategy**:

- **Add before removing** – the existing `BlueprintStrategy` / sync `run_node()` path stays live until the new path is proven.
- **Feature flag** – a single env var (`TRADE_SOURCE_MODE`) switches between old and new behaviour per deploy.
- **Each step is a separate, deployable commit** – pausable after any step.

---

## Step 1 – Async Runner Alongside the Sync One (Additive) — ✅ DONE

**Files**: `EDGETrader.py`

**Changes** (already completed, unchanged by this revision):

1. Add a coroutine `run_node_async(node) -> None` that does `await node.run_async()` inside a `try/finally` that still calls `node.stop()` / `node.dispose()` on exit.
2. **Leave** `run_node()` (sync) completely untouched — it's still what `main()` calls.
3. Do not call `run_node_async` from anywhere yet.

**Why this is safe**: purely additive; nothing in `main()` changes, so behavior is identical to today.

---

## Step 2 – Outbox Publisher: Postgres → SNS (Silent Internal)

**Files**: wherever `trade_events` rows are written today (the signal/webhook handler that calls `TradeEventsDB.insert_trade_event` — **not** the trading node itself)

**Changes**:

1. Add `boto3` SNS publish call **immediately after** a successful `insert_trade_event` commit, using the returned `event_id`:

   ```python
   sns.publish(
       TopicArn=os.environ["SNS_TRADE_EVENTS_TOPIC_ARN"],
       Message=json.dumps(row),
       MessageAttributes={
           "event_type": {"DataType": "String", "StringValue": row["event_type"]},
           "trade_id": {"DataType": "String", "StringValue": row["trade_id"]},
       },
   )
   ```

2. Add env var `SNS_TRADE_EVENTS_TOPIC_ARN`. Reuse the existing `AWS_REGION` env var / `boto3` session pattern already used for Secrets Manager.
3. Provision the SNS topic (`trade-events`) itself — infra-only, no application traffic depends on it yet.
4. **Do not** wire an SQS consumer yet — validate stand-alone (insert a dummy row, confirm the publish call succeeds and a manually-subscribed test SQS queue receives it).

**Why this is safe**: the publish call is additive to an existing write path; nothing downstream consumes it yet.

**Open risk to track (not blocking)**: the `INSERT` and the `sns.publish` are two separate operations — a crash between them means a row exists in Postgres with no corresponding SNS/SQS delivery. This is the classic outbox-pattern gap. Two options, either is acceptable to start:

- **Accept the gap initially** and add a lightweight reconciliation script later (queries `trade_events` for rows with no matching `processed_events` row older than some grace period, and re-publishes) — see Step 8.
- **Or** move the publish inside the same DB transaction using a dedicated `outbox` table + a small polling relay (heavier, skip for v1 given low trade volume).

---

## Step 3 – SQS Queue + Consumer Module (Silent Internal)

**Files**: new `trade_events_sqs.py` (or a new section in `EDGETrader.py`)

**Changes**:

1. Provision `trade-events-queue` (Standard queue — ordering across different trades isn't required; ordering *within* a trade's lifecycle is already handled by `previous_event_id` chaining and the SM), subscribed to the `trade-events` SNS topic.
2. Provision a dead-letter queue `trade-events-queue-dlq` with a redrive policy (e.g. `maxReceiveCount=5`) so a message that repeatedly fails to be claimed/handled doesn't loop forever or vanish silently.
3. Add env vars: `SQS_TRADE_EVENTS_QUEUE_URL`, `SQS_POLL_WAIT_SECONDS` (default `20`, i.e. long polling), `SQS_MAX_MESSAGES` (default `10`).
4. Implement `async def receive_trade_events(sqs_client, queue_url) -> List[dict]` (long-poll `ReceiveMessage`) and `async def delete_message(sqs_client, queue_url, receipt_handle) -> None`.
5. **Do not** wire this into `main()` yet — validate stand-alone (publish a test message via the Step 2 topic, confirm this module receives and deletes it).

**Why this is safe**: no existing code path calls this module; the live trader is untouched.

---

## Step 4 – `OrderManagementSM` and `TradeStrategy` (Additive, Unused)

**Files**: `EDGETrader.py`

**Changes** (unchanged from the original plan — transport-agnostic):

1. Implement `OrderManagementSM` with states `Idle, Validating, PlacingEntry, AwaitingFill, Protecting, InPosition, Closing` and transitions, unit-testable without Nautilus.
2. Add `TradeStrategyConfig(StrategyConfig, frozen=True)` with `instrument_id`, `bar_type`, `order_id_tag`, and a **deterministic `client_order_id`** derived from `trade_id` (this is the Binance-side dedupe backstop — see Step 5).
3. Add `TradeStrategy(Strategy)`:
   - `__init__` creates `self.sm = OrderManagementSM()`.
   - `on_start()` — feeds the initial "OpenTradeEvent" into the SM, submits the entry order using the deterministic `client_order_id`.
   - `on_order_filled` / `on_order_rejected` / `on_order_canceled` — feed the SM, drive `AwaitingFill → Protecting → InPosition → Closing → Idle`.
4. **Do not** instantiate `TradeStrategy` from `main()` yet.

**Why this is safe**: new classes sit next to the old ones; nothing references them from the live path.

---

## Step 5 – Standalone SQS Trade-Event Listener Coroutine (Additive, Unused)

**Files**: `EDGETrader.py` (or `trade_events_sqs.py`)

**Changes**:

1. Implement `async def listen_trade_events(node: TradingNode, sqs_client, queue_url) -> None`:
   - Long-poll loop: `receive_trade_events(...)`.
   - For each message: parse `event_id` from the payload, call the **existing** `TradeEventsDB.claim_event(event_id)` (`INSERT INTO processed_events ... ON CONFLICT DO NOTHING RETURNING`) — this is unchanged from the Postgres-only design and is what actually answers "have I processed this?", regardless of transport.
   - If claimed: build `TradeStrategyConfig` (`order_id_tag=trade_id`), instantiate `TradeStrategy`, `add_strategy` + `start_strategy`.
   - If not claimed (already processed): no-op — this is the expected, harmless path for SQS's at-least-once redelivery.
   - **Delete the SQS message once the event has been claimed-and-handed-off** (or recognized as a duplicate) — **not** held open until the trade fully closes. A trade can run for hours; SQS visibility timeout is not the right mechanism to track trade lifecycle, `trade_events` + the SM already do that.
   - On trade close (SM reaches terminal `Idle`): append the closing event to `trade_events` (unchanged), then `stop_strategy` / `remove_strategy`.
2. **Design note on the claim/act ordering gap**: if the process crashes *after* `claim_event` succeeds but *before* `add_strategy`/`start_strategy` completes, the event is now marked processed but no strategy exists for it — a genuine gap, not fully closed by SQS redelivery (the message may already be deleted, or if not yet deleted, a retry would see `claim_event` return nothing and skip). This is mitigated, not eliminated, by:
   - Deleting the SQS message only *after* `start_strategy` succeeds, so an early crash leaves the message undeleted and SQS *will* redeliver it — but `claim_event` will report it already claimed, so add a narrow "was it claimed but never started" reconciliation check (query `processed_events` joined against currently-active `order_id_tag`s in the node) as a follow-up hardening step once the happy path is proven in staging.
   - The deterministic `client_order_id` (Step 4) as a second, independent backstop specifically for the order-submission step.
3. Validate against a **test** SQS queue + a `node` running in `VIRTUAL` `TRADING_MODE`, off to the side (manual script), confirming strategies come up/down correctly and duplicate deliveries are correctly skipped.
4. **Do not** call this coroutine from `main()` yet.

**Why this is safe**: this remains the riskiest new piece of logic — isolating it behind "not yet called from `main()`" lets it be hammered on in isolation before it touches the real run path.

---

## Step 6 – Telegram Notifier Lambda (Additive, Independent Subscriber)

**Files**: new, separate deployable (Lambda function, not part of `EDGETrader.py`)

**Changes**:

1. Subscribe a Lambda function (`trade-events-telegram-notifier`) directly to the `trade-events` SNS topic — a second, independent fan-out branch, parallel to the SQS/trading-node path.
2. On invoke: format a human-readable message from the SNS payload (`event_type`, `trade_id`, `instrument`, `side`, `ep`/`fill_price`, `close_reason`, etc.) and call the Telegram Bot API:

   ```
   POST https://api.telegram.org/bot<TOKEN>/sendMessage
   ```

3. Store the bot token in AWS Secrets Manager (same pattern already used for Binance credentials), not as a plaintext env var.
4. **No idempotency guard needed here** — a duplicate Telegram message from an SNS redelivery is harmless (unlike a duplicate order), so this path deliberately does **not** touch `processed_events`.
5. Validate stand-alone: publish a test SNS message, confirm a Telegram message arrives; confirm a duplicate publish produces a duplicate (acceptable) message rather than an error.

**Why this is safe**: entirely separate deployable, subscribed independently; a bug here cannot affect order execution, and it can be added/removed at any time without touching `EDGETrader.py`.

---

## Step 7 – Feature Flag Wiring in `main()` (Dual-Path)

**Files**: `EDGETrader.py`

**Changes**:

1. Add `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "SINGLE").upper()` (`SINGLE` | `EVENT_DRIVEN`), validated like `TRADING_MODE` is today.
2. In `main()`, after `node` is built (factories registered, `node.build()` called — same as today):
   - If `trade_source_mode == "SINGLE"`: **unchanged** — instantiate `BlueprintStrategy`, call the existing `run_node(node, strategy, ...)`.
   - If `trade_source_mode == "EVENT_DRIVEN"`: create an SQS client (Step 3), and run `node.run_async()` and `listen_trade_events(node, sqs_client, queue_url)` **concurrently** (`asyncio.gather`), inside the async teardown pattern from Step 1.
3. Default stays `SINGLE`, so **nothing changes for existing deployments** unless the env var is explicitly set.

**Why this is safe**: the branch is additive and off by default; existing deploys keep running `BlueprintStrategy` exactly as before.

---

## Step 8 – Validate `EVENT_DRIVEN` Mode End-to-End (Staging)

**Files**: none (deployment/infra/config only)

**Changes**:

1. Deploy with `TRADE_SOURCE_MODE=EVENT_DRIVEN` and `TRADING_MODE=VIRTUAL` in a staging environment against real (or staging) SNS/SQS resources and Postgres.
2. Insert real trade rows via the outbox publisher and confirm:
   - SNS → SQS delivery works, and the trading node claims and acts on each event exactly once.
   - SNS → Lambda → Telegram delivery works independently of the trading path.
   - Duplicate SQS deliveries (simulate via a short visibility timeout) are correctly skipped by `claim_event` and produce no duplicate orders.
   - Poison messages (force a handler exception) land in the DLQ after `maxReceiveCount` retries rather than looping or vanishing.
   - Each trade gets its own `TradeStrategy`/`order_id_tag`, multiple trades run concurrently without interfering.
   - SM reaches `Closing → Idle` correctly and the strategy is removed (`order_id_tag` freed for reuse).
   - Add the reconciliation script from Step 2 (Postgres rows with no `processed_events` entry after a grace period) and confirm it correctly flags an injected "publish failed" case.
3. `SINGLE` mode in production is untouched throughout.

**Why this is safe**: purely a validation step; production traffic still runs the old path.

---

## Step 9 – Promote `EVENT_DRIVEN` to Default (Soft Switch)

**Files**: `EDGETrader.py`

**Changes**:

1. Flip the default: `trade_source_mode = os.getenv("TRADE_SOURCE_MODE", "EVENT_DRIVEN").upper()`.
2. **Keep** the `SINGLE` branch and `BlueprintStrategy` fully intact — it's now the explicit opt-out (`TRADE_SOURCE_MODE=SINGLE`) rather than the default.
3. Roll out to production behind normal deploy/monitoring practice. Watch the DLQ depth and CloudWatch alarms on it from day one.

**Why this is safe**: any regression can be instantly reverted by setting `TRADE_SOURCE_MODE=SINGLE` — no code change needed to roll back.

---

## Step 10 – Remove the Legacy Single-Strategy Path (The Big Cleanup)

**Files**: `EDGETrader.py`

**Changes**:

1. Once `EVENT_DRIVEN` has run stably in production for a full cycle of monitoring:
   - Delete `BlueprintConfig`, `BlueprintStrategy`.
   - Delete the sync `run_node()`.
   - Delete `TRADE_SOURCE_MODE` and the `SINGLE` branch in `main()`.
   - `main()` now unconditionally: builds the node, registers factories, `node.build()`, then runs `node.run_async()` concurrently with `listen_trade_events(node, sqs_client, queue_url)`.
2. Update/remove the single-instrument `bar_type` construction in `main()` if it's no longer needed globally.

**Why this is safe**: by this point the event-driven path has been the *only* thing production traffic exercises for a full validated cycle (Step 9); the legacy code is provably dead.

---

## Rollback / Validation at Each Step

| Step | How to validate | How to rollback |
| ------ | ------------------ | ------------------ |
| 1 | Import/run unit test on `run_node_async` in isolation; live trader unaffected. | No rollback needed (additive, unused). *(Done)* |
| 2 | Insert dummy row via publisher, confirm SNS publish succeeds and a test SQS queue receives it. | No rollback needed (publish call is additive; nothing consumes it yet). |
| 3 | Stand-alone script: publish via Step 2 topic, confirm this module receives + deletes the message. | No rollback needed (internal, unused). |
| 4 | Unit-test `OrderManagementSM` transitions directly. | No rollback needed (additive, unused). |
| 5 | Run `listen_trade_events` manually against test SQS + VIRTUAL node; confirm strategy add/remove and duplicate-skip behavior. | No rollback needed (additive, unused). |
| 6 | Publish test SNS message, confirm Telegram message arrives, independently of Step 5. | Remove the Lambda's SNS subscription — zero impact on the trading path. |
| 7 | Deploy with default `TRADE_SOURCE_MODE` unset — confirm behavior is byte-for-byte identical to pre-Step-7. | Ensure `TRADE_SOURCE_MODE` env var is unset/`SINGLE`. |
| 8 | Staging soak test with real trade rows, concurrent trades, duplicate delivery, DLQ. | Leave production on `SINGLE` (already true). |
| 9 | Production soak with `EVENT_DRIVEN` as default; monitor DLQ depth, claim outcomes, SM transitions, strategy cleanup. | Set `TRADE_SOURCE_MODE=SINGLE` — instant revert, no deploy needed. |
| 10 | Full app runs on event-driven path only, legacy code removed, all trades still process correctly. | Restore backed-up pre-Step-10 file. |

---

## Notes / Open Questions to Resolve Before Step 5

- **Bar subscription per trade**: unchanged from the original plan — confirm whether `instrument_provider_config` needs to load multiple instruments up front, or lazily per trade.
- **Outbox publish-failure handling**: confirm whether the Step 2 "accept the gap + reconcile later" approach is sufficient for expected trade volume, or whether a transactional outbox table is warranted before going to production (see Step 2 open risk note).
- **DLQ policy**: confirm `maxReceiveCount` and alerting — a message hitting the DLQ likely means a bug in claim/strategy startup and should page, not silently accumulate.
- **Claim-then-crash gap** (Step 5, design note): confirm whether the narrow reconciliation check (claimed-but-never-started) is needed before Step 9, or whether the deterministic `client_order_id` backstop is judged sufficient at current trade volume.
- **Credentials loading**: unchanged by this migration — `load_credentials_from_aws` / `load_ed25519_credentials_from_aws` stay exactly as in Step "Startup" of MainSD2 (including the VIRTUAL-mode skip of Ed25519 loading).
