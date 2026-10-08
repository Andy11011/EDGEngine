"""
signals_db_async.py — Postgres connectivity for edge-scanner.

Mirrors the style of trades_db_async.py (async singleton around an asyncpg pool).

Responsibilities
----------------
READ  (owned by travis-scanner, via PSM.py / Scanner.py):
    scanner_results_l1   -> latest scan = the current list of pairs to watch

WRITE (owned by edge-scanner):
    edge_signals         -> RSI crossover signals, read by travis' dashboard

edge_signals is idempotent on (symbol, timeframe, signal_type, bar_time), so
restarts / historical backfill / reconnects can never create duplicates.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import asyncpg

NOTIFY_CHANNEL = "edge_signal"


@dataclass(frozen=True)
class TargetPairs:
    """Snapshot of travis-scanner's latest scan."""
    scan_id: int
    scanned_at: datetime      # naive local time, as written by Scanner.py
    pairs: List[str]          # Binance format, e.g. "BTCUSDT"


class SignalsDB:
    """Async singleton wrapper around an asyncpg connection pool."""

    _instance: Optional["SignalsDB"] = None
    _init_lock = asyncio.Lock()

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def get_instance(cls) -> "SignalsDB":
        if cls._instance is not None:
            return cls._instance
        async with cls._init_lock:
            if cls._instance is None:
                print("Connecting to Postgres (edge_signals)...", file=sys.stderr)
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

    # ------------------------------------------------------------------
    # Schema (only tables owned by edge-scanner)
    # ------------------------------------------------------------------
    async def _init_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS edge_signals (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(32) NOT NULL,          -- Binance format, matches travis ("BTCUSDT")
                    signal_type VARCHAR(16) NOT NULL,     -- 'OB_CROSS' | 'OS_CROSS'
                    timeframe VARCHAR(16) NOT NULL,       -- e.g. '15-MINUTE'
                    rsi DOUBLE PRECISION NOT NULL,
                    close_price NUMERIC NOT NULL,
                    bar_time TIMESTAMPTZ NOT NULL,        -- bar open time (UTC)
                    source VARCHAR(12) NOT NULL,          -- 'live' | 'historical'
                    payload JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (symbol, timeframe, signal_type, bar_time)
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_edge_signals_bar_time ON edge_signals(bar_time)"
            )
        print("✅ edge_signals table ready", file=sys.stderr)

    # ------------------------------------------------------------------
    # READ from travis-scanner: which pairs should we watch?
    # ------------------------------------------------------------------
    async def get_target_pairs(
        self,
        pair_types: Iterable[str] = ("filtered", "newcomer"),
    ) -> Optional[TargetPairs]:
        """
        Return the pairs from the most recent row of scanner_results_l1, or
        None if travis hasn't produced any results yet (empty/missing table).

        Notes on travis' behaviour (PSM.insert_record / Scanner.print_results):
          - one row per completed scan, newest = highest id
          - rows are only written when the scan produced >= 1 pair, so if a
            scan yields nothing the previous row stays "latest"
          - interrupted scans write nothing
        """
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    "SELECT id, result, timestamp FROM scanner_results_l1 ORDER BY id DESC LIMIT 1"
                )
            except asyncpg.UndefinedTableError:
                return None
        if row is None:
            return None

        result = row["result"]
        if isinstance(result, (str, bytes)):          # asyncpg returns JSONB as str by default
            result = json.loads(result)

        wanted = {t.lower() for t in pair_types}
        seen: set = set()
        pairs: List[str] = []
        for item in result or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("pair_name", "")).strip().upper()
            ptype = str(item.get("pair_type", "")).lower()
            if name and ptype in wanted and name not in seen:
                seen.add(name)
                pairs.append(name)

        return TargetPairs(scan_id=row["id"], scanned_at=row["timestamp"], pairs=pairs)

    # ------------------------------------------------------------------
    # WRITE edge signals
    # ------------------------------------------------------------------
    async def insert_signal(
        self,
        *,
        symbol: str,
        signal_type: str,
        timeframe: str,
        bar_time_ms: int,
        rsi: float,
        close: float,
        source: str,
        payload: Optional[Dict[str, Any]] = None,
        notify: bool = True,
    ) -> Optional[int]:
        """
        Idempotent insert. Returns the new row id, or None if the signal
        already existed (duplicate). Optionally fires pg_notify in the same
        transaction so listeners (future SSE endpoint) wake up on commit.
        """
        bar_time = datetime.fromtimestamp(bar_time_ms / 1000, tz=timezone.utc)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row_id = await conn.fetchval(
                    """
                    INSERT INTO edge_signals
                        (symbol, signal_type, timeframe, rsi, close_price, bar_time, source, payload)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                    ON CONFLICT (symbol, timeframe, signal_type, bar_time) DO NOTHING
                    RETURNING id
                    """,
                    symbol, signal_type, timeframe, rsi, close, bar_time, source,
                    json.dumps(payload or {}),
                )
                if row_id is not None and notify:
                    await conn.execute("SELECT pg_notify($1, $2)", NOTIFY_CHANNEL, str(row_id))
                return row_id

    async def get_recent_signals(
        self,
        since_id: int = 0,
        limit: int = 200,
        ttl_hours: int = 48,
    ) -> List[Dict[str, Any]]:
        """Newest first. Handy for edge-side tooling; travis can run the same SQL via PSM."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, symbol, signal_type, timeframe, rsi, close_price,
                       bar_time, source, created_at
                FROM edge_signals
                WHERE id > $1 AND bar_time > $2
                ORDER BY id DESC
                LIMIT $3
                """,
                since_id, cutoff, limit,
            )
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # TTL
    # ------------------------------------------------------------------
    async def purge_expired(self, ttl_hours: int = 48, batch_size: int = 10_000) -> int:
        """Delete signals whose bar_time is older than the TTL, in batches."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
        total = 0
        async with self.pool.acquire() as conn:
            while True:
                result = await conn.execute(
                    """
                    DELETE FROM edge_signals
                    WHERE id IN (
                        SELECT id FROM edge_signals WHERE bar_time < $1 ORDER BY id LIMIT $2
                    )
                    """,
                    cutoff, batch_size,
                )
                deleted = int(result.split()[-1])      # "DELETE 123"
                total += deleted
                if deleted < batch_size:
                    return total

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def close(self) -> None:
        await self.pool.close()
        SignalsDB._instance = None
