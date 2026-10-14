"""Pair reconciler: keeps the watched pair set in sync with travis-scanner's latest scan.

Polls `scanner_results_l1` (via signals_db_async.SignalsDB), then adds / removes
pairs on a running strategy. The strategy only has to satisfy the `PairWatcher`
protocol below, so this module has no dependency on EDGEScanner.py.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Optional, Protocol

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.model.identifiers import InstrumentId

from signals_db_async import SignalsDB


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


class PairWatcher(Protocol):
    """What the reconciler needs from a strategy."""

    @property
    def is_running(self) -> bool: ...

    @property
    def watched(self) -> set[str]: ...

    def watch(self, instrument_id: InstrumentId) -> bool: ...

    def unwatch(self, instrument_id: InstrumentId) -> None: ...


class PairReconciler:
    def __init__(
        self,
        watcher: PairWatcher,
        *,
        pair_types: tuple[str, ...],
        poll_seconds: float,
        remove_after_misses: int,
        add_stagger_seconds: float,
    ):
        self._watcher = watcher
        self._pair_types = pair_types
        self._poll = poll_seconds
        self._remove_after = remove_after_misses
        self._stagger = add_stagger_seconds

        self._misses: dict[str, int] = {}
        self._last_scan_id: Optional[int] = None

    async def run(self) -> None:
        # Wait until the node has connected the data client (instruments are in
        # the cache) and started the strategy.
        while not self._watcher.is_running:
            await asyncio.sleep(1)

        db = await SignalsDB.get_instance()
        while True:
            try:
                await self._reconcile_once(db)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"❌ Pair reconcile failed: {e!r}")
            await asyncio.sleep(self._poll)

    async def _reconcile_once(self, db: SignalsDB) -> None:
        snap = await db.get_target_pairs(self._pair_types)
        if snap is None or not snap.pairs:
            # Never tear everything down just because travis has nothing (yet)
            log(f"ℹ️ No pairs from travis-scanner yet; keeping current set ({len(self._watcher.watched)} active)")
            return

        if snap.scan_id != self._last_scan_id:
            log(f"📥 travis scan #{snap.scan_id} @ {snap.scanned_at}: {len(snap.pairs)} pairs")
            self._last_scan_id = snap.scan_id

        desired = set(snap.pairs)
        watched = self._watcher.watched

        # ---- add ----
        for sym in snap.pairs:
            self._misses.pop(sym, None)
            if sym in watched:
                continue
            try:
                iid = InstrumentId.from_str(f"{sym}.{BINANCE}")
            except Exception:
                log(f"⚠️ Skipping invalid symbol from travis: {sym!r}")
                continue
            if self._watcher.watch(iid):
                log(f"➕ Watching {sym} ({len(self._watcher.watched)} active)")
                await asyncio.sleep(self._stagger)   # be gentle with Binance kline requests
            else:
                log(f"⚠️ {sym} not available on Binance spot; will retry next poll")

        # ---- remove (grace period so a pair that flaps out for one scan isn't churned) ----
        for sym in self._watcher.watched - desired:
            self._misses[sym] = self._misses.get(sym, 0) + 1
            if self._misses[sym] >= self._remove_after:
                self._watcher.unwatch(InstrumentId.from_str(f"{sym}.{BINANCE}"))
                self._misses.pop(sym, None)
                log(f"➖ Stopped watching {sym} ({len(self._watcher.watched)} active)")
