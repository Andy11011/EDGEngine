"""
trade_events_db.py — Postgres connectivity for the event‑sourced trade log.

Updated to match the final ER:
- trade_events (immutable event log, self‑referencing)
- order_fills (fill details)
- claimed_events (dedup ledger keyed by ticker+event_type+occurred_at)

All methods are async and use asyncpg.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any

import asyncpg


class TradeEventsDB:
    """Async singleton wrapper around an asyncpg connection pool."""

    _instance: Optional["TradeEventsDB"] = None
    _init_lock = asyncio.Lock()

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def get_instance(cls) -> "TradeEventsDB":
        if cls._instance is not None:
            return cls._instance
        async with cls._init_lock:
            if cls._instance is None:
                print("Connecting to Postgres (trade_events)...", file=sys.stderr)
                pool = await asyncpg.create_pool(
                    database=os.getenv("DB_NAME", "postgres"),
                    user=os.getenv("DB_USER", "user"),
                    password=os.getenv("DB_PASSWORD", "pass"),
                    host=os.getenv("DB_HOST", "productiondb"),
                    port=int(os.getenv("DB_PORT", "5432")),
                    min_size=1,
                    max_size=int(os.getenv("DB_POOL_MAX_SIZE", "5")),
                )
                instance = cls(pool)
                await instance._init_schema()
                cls._instance = instance
        return cls._instance

    async def _init_schema(self) -> None:
        """Create tables according to the final ER diagram."""
        async with self.pool.acquire() as conn:
            # 1. trade_events – immutable event log with self‑reference
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_events (
                    event_id BIGSERIAL PRIMARY KEY,
                    trade_id VARCHAR(64) NOT NULL,
                    previous_event_id BIGINT REFERENCES trade_events(event_id),
                    event_type VARCHAR(20) NOT NULL,
                    instrument VARCHAR(64) NOT NULL,
                    side VARCHAR(8) NOT NULL,
                    size NUMERIC NOT NULL,
                    ep NUMERIC,
                    sl NUMERIC,
                    tp NUMERIC,
                    market_buy_order_id VARCHAR,
                    market_sell_order_id VARCHAR,
                    limit_buy_order_id VARCHAR,
                    limit_sell_order_id VARCHAR,
                    stop_limit_buy_order_id VARCHAR,
                    stop_limit_sell_order_id VARCHAR,
                    oco_order_id VARCHAR,
                    fill_price NUMERIC,
                    close_reason VARCHAR,
                    cancel_reason VARCHAR,
                    metadata JSONB,
                    occurred_at TIMESTAMP NOT NULL,
                    target VARCHAR(10) NOT NULL DEFAULT 'virtual'
                )
            """)
            # Migration for pre-existing tables created before `target` existed
            # (CREATE TABLE IF NOT EXISTS won't add columns to an existing
            # table, so this ALTER makes the upgrade idempotent either way).
            await conn.execute("""
                ALTER TABLE trade_events
                ADD COLUMN IF NOT EXISTS target VARCHAR(10) NOT NULL DEFAULT 'virtual'
            """)

            # 2. order_fills – linked by trade_id
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS order_fills (
                    fill_id BIGINT PRIMARY KEY,
                    trade_id VARCHAR(64) NOT NULL,
                    order_id VARCHAR NOT NULL,
                    price NUMERIC NOT NULL,
                    qty NUMERIC NOT NULL,
                    quote_qty NUMERIC NOT NULL,
                    commission NUMERIC,
                    commission_asset VARCHAR,
                    is_maker BOOLEAN NOT NULL,
                    filled_at TIMESTAMP NOT NULL
                )
            """)

            # 3. claimed_events – infrastructure for idempotent consumption
            #    Keyed by the exact trade identity: ticker + event_type +
            #    occurred_at + target. `target` is part of the key (not just
            #    a column) because a single SQS event can now legitimately
            #    open both a virtual AND a real trade — those are two
            #    separate claims, not duplicates of each other.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS claimed_events (
                    ticker VARCHAR(64) NOT NULL,
                    event_type VARCHAR(20) NOT NULL,   -- 'open' or 'cancel'
                    occurred_at TIMESTAMP NOT NULL,
                    target VARCHAR(10) NOT NULL DEFAULT 'virtual',
                    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, event_type, occurred_at, target)
                )
            """)
            # Migration for pre-existing installs: add the column, then widen
            # the primary key to include it. Dropping/re-adding the PK is
            # only safe because claimed_events is a pure dedup ledger (no
            # foreign keys reference it) — existing rows get 'virtual' as a
            # reasonable default via DEFAULT, since every trade was
            # virtual-or-real-but-singular before this change.
            await conn.execute("""
                ALTER TABLE claimed_events
                ADD COLUMN IF NOT EXISTS target VARCHAR(10) NOT NULL DEFAULT 'virtual'
            """)
            await conn.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'claimed_events_pkey'
                        AND conrelid = 'claimed_events'::regclass
                        AND array_length(conkey, 1) = 3
                    ) THEN
                        ALTER TABLE claimed_events DROP CONSTRAINT claimed_events_pkey;
                        ALTER TABLE claimed_events ADD PRIMARY KEY (ticker, event_type, occurred_at, target);
                    END IF;
                END $$;
            """)

            # 4. trades_config – singleton row of tunable sizing config.
            #    risk_ratio matches LevelsBot's "Risk Ratio (%)" input (stored
            #    here as a fraction, e.g. 0.001 = 0.1%, not a percentage).
            #    virtual_balance_usdt is the equity used for sizing in
            #    virtual/simulated modes (TEST1/TEST2); real modes (TEST3/LIVE)
            #    size off the live account balance instead and ignore this.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trades_config (
                    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    risk_ratio NUMERIC NOT NULL DEFAULT 0.001,
                    virtual_balance_usdt NUMERIC NOT NULL DEFAULT 500,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Seed the single config row on first run only — never overwrite
            # an existing row (that's what update_trades_config is for).
            await conn.execute("""
                INSERT INTO trades_config (id, risk_ratio, virtual_balance_usdt)
                VALUES (1, 0.001, 500)
                ON CONFLICT (id) DO NOTHING
            """)

            # 5. node_heartbeats — one row per (venue, target). Written
            #    periodically by each headless node process (binance_real_node.py,
            #    binance_virtual_mainnet_node.py) since those no longer expose an
            #    HTTP /health endpoint of their own (per MultiVenueTOD.md: exec-based
            #    health only). edge-api.py reads this table to answer /health and
            #    /balance/mainnet — it has no direct reference to the node processes.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS node_heartbeats (
                    venue VARCHAR(20) NOT NULL,
                    target VARCHAR(10) NOT NULL,
                    is_running BOOLEAN NOT NULL,
                    balance_usdt NUMERIC,
                    detail VARCHAR,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (venue, target)
                )
            """)

            # 6. cancel_requests — how edge-api.py (DB-pool-only, no AWS
            #    creds, no reference to the node processes) asks a node
            #    process to cancel a trade. The node processes poll this
            #    table alongside their SQS queue and mark rows processed
            #    once handled. This keeps edge-api a pure read/DB service
            #    per MultiVenueTOD.md instead of giving it SQS send access.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cancel_requests (
                    id BIGSERIAL PRIMARY KEY,
                    trade_id VARCHAR(64) NOT NULL,
                    target VARCHAR(10) NOT NULL,
                    requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN NOT NULL DEFAULT FALSE,
                    processed_at TIMESTAMP
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_cancel_requests_pending ON cancel_requests(target, processed)")

            # Indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id ON trade_events(trade_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_occurred_at ON trade_events(occurred_at)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_order_fills_trade_id ON order_fills(trade_id)")

        print("✅ Event‑sourcing tables ready", file=sys.stderr)

    # ------------------------------------------------------------------
    # Claim an SQS message (dedup)
    # ------------------------------------------------------------------
    async def claim_event(self, ticker: str, event_type: str, occurred_at: str, target: str = "virtual") -> bool:
        """
        Claim (ticker, event_type, occurred_at, target). `target` is part of
        the dedup key so one SQS event that fans out to both a virtual and a
        real trade produces two independent claims, not a duplicate.
        """
        # Parse ISO string with Z (UTC) and then discard timezone info
        dt = datetime.fromisoformat(occurred_at.replace('Z', '+00:00')).replace(tzinfo=None)
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(
                """INSERT INTO claimed_events (ticker, event_type, occurred_at, target)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                RETURNING ticker""",
                ticker, event_type, dt, target
            )
            return result is not None

    # ------------------------------------------------------------------
    # Undo a claim (used when processing fails after claiming, so the same
    # SQS message can be legitimately retried instead of being silently
    # treated as a duplicate on redelivery)
    # ------------------------------------------------------------------
    async def unclaim_event(self, ticker: str, event_type: str, occurred_at: str, target: str = "virtual") -> bool:
        """
        Delete a previously-inserted claim row for (ticker, event_type, occurred_at, target).
        Returns True if a row was actually deleted, False if there was nothing to delete.
        """
        dt = datetime.fromisoformat(occurred_at.replace('Z', '+00:00')).replace(tzinfo=None)
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """DELETE FROM claimed_events
                   WHERE ticker = $1 AND event_type = $2 AND occurred_at = $3 AND target = $4""",
                ticker, event_type, dt, target
            )
            # asyncpg's execute() returns a status string like "DELETE 1"
            deleted_count = int(result.split()[-1])
            return deleted_count > 0

    # ------------------------------------------------------------------
    # Insert a new trade event (used by the strategy)
    # ------------------------------------------------------------------
    async def insert_trade_event(
        self,
        trade_id: str,
        event_type: str,
        instrument: str,
        side: str,
        size: float,
        occurred_at: Optional[str] = None,  # ISO timestamp, default now()
        ep: Optional[float] = None,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        market_buy_order_id: Optional[str] = None,
        market_sell_order_id: Optional[str] = None,
        limit_buy_order_id: Optional[str] = None,
        limit_sell_order_id: Optional[str] = None,
        stop_limit_buy_order_id: Optional[str] = None,
        stop_limit_sell_order_id: Optional[str] = None,
        oco_order_id: Optional[str] = None,
        fill_price: Optional[float] = None,
        close_reason: Optional[str] = None,
        cancel_reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        previous_event_id: Optional[int] = None,
        target: str = "virtual",
    ) -> Dict[str, Any]:
        """
        Insert a new event into trade_events. `target` ('virtual' or 'real')
        records which execution path this trade actually ran on — this is
        the event-sourced audit trail for virtual-vs-real, so no separate
        "mode" table is needed elsewhere.
        If previous_event_id is omitted, it automatically links to the latest
        existing event for the same trade_id. A brand-new trade_id naturally
        has no prior rows (trade_id already encodes the unique open
        timestamp), so this correctly starts a new chain without needing to
        special-case any particular event_type.
        Returns the inserted row as a dict (including the generated event_id).
        """
        if metadata is None:
            metadata = {}
        if occurred_at is None:
            occurred_at = "CURRENT_TIMESTAMP"

        async with self.pool.acquire() as conn:
            # Auto‑link to whatever the latest event for this trade_id is (if any)
            if previous_event_id is None:
                prev = await conn.fetchval(
                    """SELECT event_id FROM trade_events
                       WHERE trade_id = $1
                       ORDER BY occurred_at DESC, event_id DESC LIMIT 1""",
                    trade_id,
                )
                previous_event_id = prev

            row = await conn.fetchrow(
                f"""
                INSERT INTO trade_events (
                    trade_id, previous_event_id, event_type, instrument, side, size,
                    ep, sl, tp,
                    market_buy_order_id, market_sell_order_id,
                    limit_buy_order_id, limit_sell_order_id,
                    stop_limit_buy_order_id, stop_limit_sell_order_id,
                    oco_order_id,
                    fill_price, close_reason, cancel_reason,
                    metadata, occurred_at, target
                ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19,
                    $20::jsonb, {occurred_at}, $21
                )
                RETURNING *
                """,
                trade_id,
                previous_event_id,
                event_type,
                instrument,
                side,
                size,
                ep,
                sl,
                tp,
                market_buy_order_id,
                market_sell_order_id,
                limit_buy_order_id,
                limit_sell_order_id,
                stop_limit_buy_order_id,
                stop_limit_sell_order_id,
                oco_order_id,
                fill_price,
                close_reason,
                cancel_reason,
                json.dumps(metadata),
                target,
            )
            return dict(row)

    # ------------------------------------------------------------------
    # Insert an order fill
    # ------------------------------------------------------------------
    async def insert_order_fill(
        self,
        fill_id: int,
        trade_id: str,
        order_id: str,
        price: float,
        qty: float,
        quote_qty: float,
        is_maker: bool,
        filled_at: Optional[str] = None,
        commission: Optional[float] = None,
        commission_asset: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Insert a fill record. Returns the inserted row, or None if fill_id already exists.
        """
        if filled_at is None:
            filled_at = "CURRENT_TIMESTAMP"

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO order_fills (
                    fill_id, trade_id, order_id, price, qty, quote_qty,
                    commission, commission_asset, is_maker, filled_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, {filled_at}
                )
                ON CONFLICT (fill_id) DO NOTHING
                RETURNING *
                """,
                fill_id, trade_id, order_id, price, qty, quote_qty,
                commission, commission_asset, is_maker,
            )
            return dict(row) if row else None

    # ------------------------------------------------------------------
    # Sizing config (trades_config) — singleton row
    # ------------------------------------------------------------------
    async def get_trades_config(self) -> Dict[str, Any]:
        """
        Returns the current sizing config: {'risk_ratio': float,
        'virtual_balance_usdt': float, 'updated_at': datetime}.

        Falls back to the same defaults as the schema seed if the row is
        somehow missing (it shouldn't be, since _init_schema seeds it), so
        sizing never hard-fails purely because of a missing config row.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT risk_ratio, virtual_balance_usdt, updated_at FROM trades_config WHERE id = 1"
            )
            if row is None:
                return {"risk_ratio": 0.001, "virtual_balance_usdt": 500.0, "updated_at": None}
            return {
                "risk_ratio": float(row["risk_ratio"]),
                "virtual_balance_usdt": float(row["virtual_balance_usdt"]),
                "updated_at": row["updated_at"],
            }

    async def update_trades_config(
        self,
        risk_ratio: Optional[float] = None,
        virtual_balance_usdt: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Partial update — only overwrites the fields explicitly passed
        (COALESCE keeps the existing value for anything left as None).
        Returns the resulting row.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE trades_config
                SET risk_ratio = COALESCE($1, risk_ratio),
                    virtual_balance_usdt = COALESCE($2, virtual_balance_usdt),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                RETURNING risk_ratio, virtual_balance_usdt, updated_at
                """,
                risk_ratio,
                virtual_balance_usdt,
            )
            return {
                "risk_ratio": float(row["risk_ratio"]),
                "virtual_balance_usdt": float(row["virtual_balance_usdt"]),
                "updated_at": row["updated_at"],
            }

    # ------------------------------------------------------------------
    # Query: find the active (open) trade for a given ticker
    # ------------------------------------------------------------------
    async def get_active_trade_for_ticker(self, ticker: str, target: str) -> Optional[str]:
        """
        Returns the trade_id of the most recent 'Opened' event for this
        (ticker, target) that does not have a subsequent terminal event
        (Closed or Cancelled) for the same trade_id.

        `target` is required (not optional) because a ticker can now have
        both an open virtual trade and an open real trade at the same time
        — without filtering by target, a 'cancel' event for one would
        ambiguously match whichever happened to be "most recent" and could
        cancel the wrong one. 'Opened' is written as soon as the entry order
        is accepted (AWAITING_FILL) — it is the start-of-chain event, not a
        post-fill one — so trades still awaiting fill are already
        discoverable here.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH latest_open AS (
                    SELECT trade_id, occurred_at
                    FROM trade_events
                    WHERE instrument = $1 AND event_type = 'Opened' AND target = $2
                    ORDER BY occurred_at DESC, event_id DESC
                    LIMIT 1
                )
                SELECT lo.trade_id
                FROM latest_open lo
                LEFT JOIN trade_events terminal
                    ON terminal.trade_id = lo.trade_id
                    AND terminal.event_type IN ('Closed', 'Cancelled')
                    AND terminal.occurred_at > lo.occurred_at
                WHERE terminal.event_id IS NULL
                """,
                ticker, target
            )
            return row["trade_id"] if row else None

    # ------------------------------------------------------------------
    # Convenience queries
    # ------------------------------------------------------------------
    async def get_latest_event_for_trade(self, trade_id: str) -> Optional[Dict[str, Any]]:
        """Return the most recent event for a given trade."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM trade_events
                WHERE trade_id = $1
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT 1
                """,
                trade_id,
            )
            return dict(row) if row else None

    async def get_all_events_for_trade(self, trade_id: str) -> List[Dict[str, Any]]:
        """Return all events for a trade in chronological order."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT *
                FROM trade_events
                WHERE trade_id = $1
                ORDER BY occurred_at ASC, event_id ASC
                """,
                trade_id,
            )
            return [dict(r) for r in rows]

    async def get_fills_for_trade(self, trade_id: str) -> List[Dict[str, Any]]:
        """Return all order fills for a trade."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM order_fills WHERE trade_id = $1 ORDER BY filled_at ASC",
                trade_id,
            )
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Node heartbeats — written by the headless node processes, read by
    # edge-api.py. Each row is fully replaced (upsert), never mutated
    # field-by-field, so a concurrent read never sees a torn write.
    # ------------------------------------------------------------------
    async def write_heartbeat(
        self,
        venue: str,
        target: str,
        is_running: bool,
        balance_usdt: Optional[float] = None,
        detail: Optional[str] = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO node_heartbeats (venue, target, is_running, balance_usdt, detail, updated_at)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (venue, target) DO UPDATE
                SET is_running = EXCLUDED.is_running,
                    balance_usdt = EXCLUDED.balance_usdt,
                    detail = EXCLUDED.detail,
                    updated_at = EXCLUDED.updated_at
                """,
                venue, target, is_running, balance_usdt, detail,
            )

    async def get_heartbeat(self, venue: str, target: str) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM node_heartbeats WHERE venue = $1 AND target = $2",
                venue, target,
            )
            return dict(row) if row else None

    async def get_all_heartbeats(self) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM node_heartbeats")
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Active trades (for edge-api's /active_trades) — derived from the
    # event log rather than in-memory process state, per MultiVenueTOD.md:
    # "Refactor /active_trades to query trade_events (deriving open trades
    # from Opened vs Closed/Cancelled) instead of reading in-memory state."
    # ------------------------------------------------------------------
    async def get_active_trades(self) -> List[Dict[str, Any]]:
        """
        Returns the latest 'Opened' event for every trade_id that has no
        subsequent terminal ('Closed' or 'Cancelled') event — i.e. every
        trade currently considered open, across both targets/venues.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT o.*
                FROM trade_events o
                WHERE o.event_type = 'Opened'
                AND NOT EXISTS (
                    SELECT 1 FROM trade_events terminal
                    WHERE terminal.trade_id = o.trade_id
                    AND terminal.event_type IN ('Closed', 'Cancelled')
                    AND terminal.occurred_at > o.occurred_at
                )
                ORDER BY o.occurred_at DESC
                """
            )
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Cancel requests — DB-based hand-off from edge-api.py (no AWS access)
    # to the node processes (which poll this table for their own target).
    # ------------------------------------------------------------------
    async def enqueue_cancel_request(self, trade_id: str, target: str) -> int:
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """INSERT INTO cancel_requests (trade_id, target)
                   VALUES ($1, $2) RETURNING id""",
                trade_id, target,
            )
            return row_id

    async def fetch_pending_cancel_requests(self, target: str) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT * FROM cancel_requests
                   WHERE target = $1 AND processed = FALSE
                   ORDER BY requested_at ASC""",
                target,
            )
            return [dict(r) for r in rows]

    async def mark_cancel_request_processed(self, request_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE cancel_requests
                   SET processed = TRUE, processed_at = CURRENT_TIMESTAMP
                   WHERE id = $1""",
                request_id,
            )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def close(self) -> None:
        await self.pool.close()
