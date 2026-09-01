Here is a short, actionable to-do list distilled from your reasoning. I’ve kept it to the hard technical decisions only:

---

**Migration / Setup To-Do List**

## Secrets → platform, not app code

- [ ] **Orchestrator decision**: Choose **ECS (Fargate/EC2)** over EKS. (Skip the $0.10/hr EKS control-plane overhead; use AWS-native primitives instead of Kubernetes operators.)
- [ ] **Secrets injection**: Configure the ECS task definition’s `secrets` block to point directly to Secrets Manager ARNs. Inject them as environment variables.
- [ ] **Code cleanup (credentials)**: Refactor `EDGETrader.py`—keep only `load_credentials_from_env()`. Delete the boto3 `load_credentials_from_aws()` path entirely (no AWS calls or IAM permissions needed inside trading containers).
- [ ] **Health checks**: Implement **`exec`-based `HEALTHCHECK`** commands in the task definition (do not rely on HTTP listeners inside the trading containers).
- [ ] **Container listener design**: Drop the HTTP listener from venue trading containers (health is purely exec-based).
- [ ] **Service discovery (optional)**: Use **ECS Service Connect** (Envoy sidecar) *only* where strictly needed (e.g., `read-api`). Do not attach it to the venue trading containers.
- [ ] **Infrastructure as Code**: Ensure all IaC (CDK/CloudFormation) uses the native ECS object model (`TaskDefinition`, `Service`, `Cluster`) with no Kubernetes compatibility layers.

## API unification → reverse proxy, not a Python service

- [ ] **Ditch the reverse-proxy fanout**: Do **not** use nginx/Envoy/ALB to route dashboard API calls to individual venue containers. (Eliminate request-time coupling to per-venue containers).
- [ ] **Adopt CQRS / read-model split**: Make venue containers **write-only** to a shared store (Postgres + optionally Redis). Build a single, dedicated **`read-api`** service that **reads-only** from this store to serve the dashboard.
- [ ] **Remove HTTP listeners from venue containers**: Delete the HTTP/FastAPI server from all venue trading containers. They no longer need to expose any port for the dashboard or platform probes.
- [ ] **Implement exec-based health checks only**: Rely exclusively on ECS’s native `HEALTHCHECK` (shell command) or Kubernetes `exec` probes for container liveness. No HTTP endpoints needed for the platform.
- [ ] **Build the new centralized `read-api` service**: Create one small, stateless FastAPI + uvicorn service. It holds only a DB connection pool—no AWS credentials, no trading logic, no references to venue containers.
- [ ] **Implement heartbeat writing in venue containers**: Make each venue container periodically write a heartbeat row (venue, target, `is_running`, `updated_at`) plus balance snapshots to the shared store (Postgres/Redis) so `read-api` can serve `/health` and `/balance` from stored state.
- [ ] **Refactor `/active_trades`**: Change it to query the `trade_events` table (deriving open trades from `Opened` vs. `Closed/Cancelled`) instead of reading in-memory process state.
- [ ] **Choose public exposure for `read-api`**: Use **API Gateway (with VPC Link)** or **Cloudflare Tunnel** for TLS termination and a stable public domain—avoid a dedicated ALB (to skip the ~$16/mo cost) unless you specifically need its L7 routing features.

## SNS fan-out with filter policies, one SQS queue per venue

- [ ] **Replace direct SQS publishing**: Change the publisher to write all trade events to a **single, centralized SNS topic** instead of directly to SQS. Ensure every message includes a `venue` message attribute.
- [ ] **Provision one dedicated SQS queue per venue**: Create a separate SQS queue for each venue (e.g., `binance-trade-events`, `bybit-trade-events`).
- [ ] **Apply SNS filter policies for routing**: Subscribe each venue-specific queue to the central SNS topic. Attach a filter policy to each subscription (e.g., `{"venue": ["binance"]}`) so SNS routes only relevant messages to each queue.
- [ ] **Update EDGETrader.py environment variables**: In each venue container's task definition, change the `SQS_TRADE_EVENTS_QUEUE_URL` env var to point to its **own dedicated queue URL** (the shared queue is gone).
- [ ] **Strip out in-code filtering**: Remove any manual `target`/`venue` filtering logic inside `receive_trade_events()`—the `receive`/`delete` code stays identical, but it no longer needs to discard messages for other venues.
- [ ] **Standardize new venue onboarding**: Adding a new venue (e.g., Bybit) is now pure **"config, not code"**—just create a new SQS queue + SNS subscription with the correct filter policy. No application code changes, no container rebuilds, no redeploys of existing venues.

## Shared tables, not per-container tables

- [ ] **Eliminate per-container tables**: Migrate to **shared, unified tables** for all venues (e.g., a single `trade_events` table instead of separate `binance_trade_events`, `bybit_trade_events`, etc.).
- [ ] **Add a `venue` column to shared tables**: Apply the same migration pattern already used for adding the `target` column (mirroring the style in lines 88–91 and 131–148 of `trades_db_async.py`).
- [ ] **Widen the primary key**: Update composite primary keys to include `venue` alongside `target` (and `id`/timestamp) to ensure uniqueness across all venues.
- [ ] **Update DB function signatures**: Refactor `claim_event()`, `unclaim_event()`, and `get_active_trade_for_ticker()` to accept a **`venue` parameter** alongside the existing `target` parameter—same shape, same logic, just expanded scope.
- [ ] **Propagate the `venue` param in caller code**: Ensure every call site (inside venue containers, the new `read-api`, and anywhere else) passes the correct `venue` string when invoking these DB functions.
