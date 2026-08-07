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

            # 3. claimed_events – infrastructure for idempotent consumption
            #    Keyed by the exact SQS message identity: ticker + event_type + occurred_at
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS claimed_events (
                    ticker VARCHAR(64) NOT NULL,
                    event_type VARCHAR(20) NOT NULL,   -- 'open' or 'cancel'
                    occurred_at TIMESTAMP NOT NULL,
                    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, event_type, occurred_at)
                )
            """)

            # Indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id ON trade_events(trade_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_occurred_at ON trade_events(occurred_at)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_order_fills_trade_id ON order_fills(trade_id)")

        print("✅ Event‑sourcing tables ready", file=sys.stderr)

    # ------------------------------------------------------------------
    # Claim an SQS message (dedup)
    # ------------------------------------------------------------------
    async def claim_event(self, ticker: str, event_type: str, occurred_at: str) -> bool:
        # Parse ISO string with Z (UTC) and then discard timezone info
        dt = datetime.fromisoformat(occurred_at.replace('Z', '+00:00')).replace(tzinfo=None)
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(
                """INSERT INTO claimed_events (ticker, event_type, occurred_at)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
                RETURNING ticker""",
                ticker, event_type, dt
            )
            return result is not None

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
    ) -> Dict[str, Any]:
        """
        Insert a new event into trade_events.
        If previous_event_id is omitted, it automatically links to the latest event
        for the same trade_id (except for 'Opened' events, which start a new chain).
        Returns the inserted row as a dict (including the generated event_id).
        """
        if metadata is None:
            metadata = {}
        if occurred_at is None:
            occurred_at = "CURRENT_TIMESTAMP"

        async with self.pool.acquire() as conn:
            # Auto‑link previous event unless it's an Opened event (new chain)
            if previous_event_id is None and event_type != "Opened":
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
    # Query: find the active (open) trade for a given ticker
    # ------------------------------------------------------------------
    async def get_active_trade_for_ticker(self, ticker: str) -> Optional[str]:
        """
        Returns the trade_id of the most recent 'Opened' event for this ticker
        that does not have a subsequent terminal event (Closed or Cancelled)
        for the same trade_id.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH latest_open AS (
                    SELECT trade_id, occurred_at
                    FROM trade_events
                    WHERE instrument = $1 AND event_type = 'Opened'
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
                ticker
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
    # Shutdown
    # ------------------------------------------------------------------
    async def close(self) -> None:
        await self.pool.close()
