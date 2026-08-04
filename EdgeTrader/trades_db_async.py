"""
trade_events_db.py — Postgres connectivity for the event‑sourced trade log.

Updated to match the corrected ER diagram (no separate TRADES table):
- trade_events (immutable event log, self‑referencing for chaining)
- order_fills (fill details)
- processed_events (infrastructure for consumer offset)

All methods are async and use asyncpg.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Optional, List, Dict, Any

import asyncpg


TRADE_EVENTS_POLL_SECONDS = int(os.getenv("TRADE_EVENTS_POLL_SECONDS", "60"))


class TradeEventsDB:
    """
    Async singleton wrapper around an asyncpg connection pool.
    """

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
        """Create tables according to the corrected ER diagram."""
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
                    occurred_at TIMESTAMP NOT NULL
                )
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

            # 3. processed_events – infrastructure for idempotent consumption
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id BIGINT PRIMARY KEY REFERENCES trade_events(event_id),
                    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id ON trade_events(trade_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_occurred_at ON trade_events(occurred_at)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_order_fills_trade_id ON order_fills(trade_id)")

        print("✅ Event‑sourcing tables ready", file=sys.stderr)

    # ------------------------------------------------------------------
    # Insert a new event
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
    ) -> Dict[str, Any]:
        """
        Insert a new event into trade_events.
        If previous_event_id is omitted, it automatically links to the latest event
        for the same trade_id (except for 'Created' events, which start a new chain).
        Returns the inserted row as a dict (including the generated event_id).
        """
        if metadata is None:
            metadata = {}
        if occurred_at is None:
            occurred_at = "CURRENT_TIMESTAMP"

        async with self.pool.acquire() as conn:
            # Auto‑link previous event unless it's a Created event
            if previous_event_id is None and event_type != "Created":
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
                    metadata, occurred_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12, $13, $14, $15,
                    $16, $17, $18, $19,
                    $20::jsonb, {occurred_at}
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
    # Consumer offset / polling helpers
    # ------------------------------------------------------------------
    async def get_unprocessed_events(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch events that have not yet been processed, ordered by occurred_at.
        Used by the poll‑fallback loop.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT e.*
                FROM trade_events e
                LEFT JOIN processed_events p ON e.event_id = p.event_id
                WHERE p.event_id IS NULL
                ORDER BY e.occurred_at ASC, e.event_id ASC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]

    async def get_pending_trade_ids(self, limit: int = 10) -> List[str]:
        """
        Returns trade_ids that have at least one unprocessed event.
        Useful for prioritising trades.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT e.trade_id
                FROM trade_events e
                LEFT JOIN processed_events p ON e.event_id = p.event_id
                WHERE p.event_id IS NULL
                ORDER BY MIN(e.occurred_at) ASC
                LIMIT $1
                """,
                limit,
            )
            return [r["trade_id"] for r in rows]

    async def claim_event(self, event_id: int) -> Optional[Dict[str, Any]]:
        """
        Atomically mark an event as processed by inserting into processed_events.
        Returns the event data if the claim succeeded (i.e. event was not already processed),
        else None.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH claimed AS (
                    INSERT INTO processed_events (event_id)
                    VALUES ($1)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id
                )
                SELECT e.*
                FROM trade_events e
                JOIN claimed c ON e.event_id = c.event_id
                """,
                event_id,
            )
            return dict(row) if row else None

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
    # Shutdown
    # ------------------------------------------------------------------
    async def close(self) -> None:
        await self.pool.close()

    # ------------------------------------------------------------------
    # LISTEN/NOTIFY stub (unchanged)
    # ------------------------------------------------------------------
    async def add_notify_listener(self, channel: str, callback) -> asyncpg.Connection:
        """
        Registers a callback on a channel via asyncpg's native listener API.
        IMPORTANT: the returned connection must be kept alive for the listener to work.
        """
        conn = await self.pool.acquire()
        await conn.add_listener(channel, callback)
        return conn
