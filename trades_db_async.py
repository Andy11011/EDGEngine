"""
trade_events_db.py — Postgres connectivity for the `trade_events` table.

Integration Map / Step 2 (Postgres Connectivity Module — Silent Internal):
This module is ADDITIVE and NOT wired into EDGETrader.py's main() yet.
Nothing in the live trader calls anything here.

Design note — why this is `asyncpg`, not PSM.py's `psycopg2` pattern:
EDGETrader.py's node will run inside ONE asyncio event loop (Step 1),
and Step 4 needs a Postgres LISTEN/NOTIFY loop running concurrently
alongside it. `psycopg2` has no native asyncio integration — bridging it
with `asyncio.to_thread(...)` works for one-off calls, but:
  1. psycopg2 connections/cursors are NOT thread-safe for concurrent use,
     and Step 4 runs the NOTIFY path and the poll-fallback path at the
     same time — a shared cursor across worker threads is a real race,
     not a hypothetical one.
  2. psycopg2 has no async LISTEN/NOTIFY hook; you'd have to manually
     poll the raw socket with `select()` inside the loop.
`asyncpg` solves both: a real connection pool (`asyncpg.create_pool`)
with proper `async with pool.acquire()` scoping instead of one shared
cursor, and a native `conn.add_listener(channel, callback)` API for
Step 4 to build on directly.

The class below keeps the same *shape* as PSM.py (lazy singleton,
schema-ensure on first connect, dict-like rows) — just async throughout.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Optional

import asyncpg


TRADE_EVENTS_POLL_SECONDS = int(os.getenv("TRADE_EVENTS_POLL_SECONDS", "60"))


class TradeEventsDB:
    """
    Async singleton wrapper around an asyncpg connection pool for the
    `trade_events` table.

    Use `await TradeEventsDB.get_instance()` to obtain it — not the
    constructor directly, since establishing the pool requires an
    `await`. A module-level asyncio.Lock guards first-time creation so
    concurrent callers (e.g. the listener and the poll loop starting up
    together in Step 4) don't race to create two pools.
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
        """Ensure the trade_events table exists (additive, no-op if present)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_events (
                    id SERIAL PRIMARY KEY,
                    trade_id VARCHAR(64) UNIQUE NOT NULL,
                    instrument VARCHAR(64) NOT NULL,
                    side VARCHAR(8) NOT NULL,
                    size NUMERIC NOT NULL,
                    sl NUMERIC,
                    tp NUMERIC,
                    payload JSONB,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        print("✅ trade_events table ready", file=sys.stderr)

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------
    async def get_pending_trade_ids(self, limit: int = 10) -> list[int]:
        """
        Poll-fallback query (Step 4): find candidate rows still `pending`,
        e.g. missed by NOTIFY during a reconnect window.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM trade_events WHERE status = 'pending' "
                "ORDER BY id ASC LIMIT $1",
                limit,
            )
            return [r["id"] for r in rows]

    async def claim_trade(self, trade_row_id: int) -> Optional[dict]:
        """
        Atomically claim a single row by id:
            UPDATE trade_events SET status='processing'
            WHERE id=$1 AND status='pending'
            RETURNING *

        Returns None if some other path (NOTIFY vs. poll) already claimed
        it first — this row-level atomicity is what de-dupes the NOTIFY
        path against the poll-fallback path per MainSD2. Safe under
        concurrency because each call acquires its own pooled connection
        and Postgres enforces the row-level atomicity, not app code.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE trade_events
                SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND status = 'pending'
                RETURNING id, trade_id, instrument, side, size, sl, tp, payload, status
                """,
                trade_row_id,
            )
            return dict(row) if row else None

    async def mark_closed(self, trade_row_id: int) -> None:
        """Called once a TradeStrategy's OrderManagementSM reaches terminal Idle."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE trade_events SET status = 'closed', updated_at = CURRENT_TIMESTAMP "
                "WHERE id = $1",
                trade_row_id,
            )

    async def insert_trade_event(
        self,
        trade_id: str,
        instrument: str,
        side: str,
        size: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        payload: Optional[dict] = None,
    ) -> dict:
        """
        Dev/test helper only — inserts a synthetic `pending` row. Used by
        the Step 2 stand-alone validation script, NOT by the live trader
        (real rows are written by whatever upstream system produces trades).
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO trade_events (trade_id, instrument, side, size, sl, tp, payload, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'pending')
                RETURNING id, trade_id, instrument, side, size, sl, tp, payload, status
                """,
                trade_id, instrument, side, size, sl, tp, json.dumps(payload or {}),
            )
            return dict(row)

    async def close(self) -> None:
        await self.pool.close()

    # ------------------------------------------------------------------
    # Step 4 preview — native LISTEN/NOTIFY (stubbed, not called yet)
    # ------------------------------------------------------------------
    async def add_notify_listener(self, channel: str, callback) -> asyncpg.Connection:
        """
        Registers `callback(conn, pid, channel, payload)` on a channel via
        asyncpg's native listener API.

        NOTE: a listening connection should NOT be a pooled connection
        that gets released/reused — it needs to stay open and dedicated
        for the life of the listener, or the registration is silently
        lost when the connection returns to the pool. Step 4 should
        acquire a connection via `self.pool.acquire()` and hold onto it
        for the listener's lifetime (releasing it explicitly on
        shutdown), rather than using the `async with` pattern used
        elsewhere in this file. Left as a stub here — not wired up yet.
        """
        conn = await self.pool.acquire()
        await conn.add_listener(channel, callback)
        return conn