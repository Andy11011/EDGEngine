"""Per‑trade strategy and state machine for event‑driven execution.

This module defines:
- `OrderManagementSM`: a pure state machine for a single trade.
- `TradeStrategyConfig`: configuration for a per‑trade strategy instance.
- `TradeStrategy`: a Nautilus `Strategy` subclass that owns one SM instance.

All classes are additive and not yet wired into `EDGETrader.py`.
"""

from __future__ import annotations

import asyncio
import json
from enum import Enum, auto
from typing import Optional, Dict, Any, Callable

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events import (
    OrderAccepted,
    OrderCancelRejected,
    OrderCanceled,
    OrderDenied,
    OrderFilled,
    OrderRejected,
    OrderSubmitted,
)
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from nautilus_trader.trading.strategy import Strategy

# We'll import the DB lazily inside the strategy to avoid circular imports
# from trades_db_async import TradeEventsDB

# A market exit first cancels the resting SL/TP orders and waits for the venue to
# confirm. If a confirmation never arrives, exit anyway after this many seconds.
PROTECTION_CANCEL_TIMEOUT_S = 15.0


# ----------------------------------------------------------------------
# State Machine
# ----------------------------------------------------------------------

class TradeState(Enum):
    """States of a single trade lifecycle.

    No IDLE state: an OrderManagementSM starts life already VALIDATING.
    CLOSING and CANCELING double as terminal states (there is no separate
    CLOSED/CANCELLED state) — once a trade reaches CLOSING or CANCELING it
    does not transition further.
    """
    VALIDATING = auto()
    PLACING_ENTRY = auto()
    AWAITING_FILL = auto()
    PROTECTING = auto()
    IN_POSITION = auto()
    CLOSING = auto()
    CANCELING = auto()


class OrderManagementSM:
    """
    Pure state machine for one trade.
    No external dependencies – tested in isolation.
    """

    #: States that do not transition any further once entered.
    TERMINAL_STATES = (TradeState.CLOSING, TradeState.CANCELING)

    def __init__(self):
        self.state = TradeState.VALIDATING
        self.trade_id: Optional[str] = None
        self.entry_order_id: Optional[str] = None
        self.close_order_id: Optional[str] = None
        self.extra_data: Dict[str, Any] = {}

    # ---------- Transitions ----------

    def validate_passed(self, trade_id: str, entry_order_id: str) -> None:
        """Validation succeeded → ready to submit the entry order."""
        if self.state != TradeState.VALIDATING:
            raise ValueError(f"validate_passed only from VALIDATING, not {self.state}")
        self.trade_id = trade_id
        self.entry_order_id = entry_order_id
        self.state = TradeState.PLACING_ENTRY

    def validate_failed(self, reason: str = "") -> None:
        """Validation failed (e.g. bad size/price, risk limits) → cancel."""
        if self.state != TradeState.VALIDATING:
            raise ValueError(f"validate_failed only from VALIDATING, not {self.state}")
        self.state = TradeState.CANCELING
        self.extra_data["cancel_reason"] = reason or "validation_failed"

    def entry_accepted(self) -> None:
        """Entry order accepted by exchange → waiting for fill."""
        if self.state != TradeState.PLACING_ENTRY:
            raise ValueError(f"entry_accepted only from PLACING_ENTRY, not {self.state}")
        self.state = TradeState.AWAITING_FILL

    def entry_filled(self) -> None:
        """Entry order filled → position open → place protection orders."""
        if self.state != TradeState.AWAITING_FILL:
            raise ValueError(f"entry_filled only from AWAITING_FILL, not {self.state}")
        self.state = TradeState.PROTECTING

    def entry_rejected(self, reason: str = "") -> None:
        """Entry order rejected → trade cancelled."""
        if self.state not in (TradeState.PLACING_ENTRY, TradeState.AWAITING_FILL):
            raise ValueError(f"entry_rejected only from PLACING_ENTRY/AWAITING_FILL, not {self.state}")
        self.state = TradeState.CANCELING
        self.extra_data["cancel_reason"] = reason or "entry_rejected"

    def entry_cancelled(self, reason: Optional[str] = None) -> None:
        """Entry order cancelled (user request, scanner signal, ...) → cancelled.
        `reason` is what ends up in trade_events.cancel_reason; the legacy
        "entry_cancelled" is kept when the caller gives none."""
        if self.state != TradeState.AWAITING_FILL:
            raise ValueError(f"entry_cancelled only from AWAITING_FILL, not {self.state}")
        self.state = TradeState.CANCELING
        self.extra_data["cancel_reason"] = reason or "entry_cancelled"

    def protection_placed(self) -> None:
        """Stop‑loss and take‑profit orders placed → position active."""
        if self.state != TradeState.PROTECTING:
            raise ValueError(f"protection_placed only from PROTECTING, not {self.state}")
        self.state = TradeState.IN_POSITION

    def protection_filled(self) -> None:
        """Protection order (SL/TP) filled → position closed."""
        if self.state != TradeState.IN_POSITION:
            raise ValueError(f"protection_filled only from IN_POSITION, not {self.state}")
        self.state = TradeState.CLOSING
        self.extra_data["close_reason"] = "protection_filled"

    def close_order_submitted(self, close_order_id: str, reason: str = "user_close") -> None:
        """Requested close (user, scanner signal, ...): submit market/limit order.
        `reason` is what ends up in trade_events.close_reason."""
        if self.state != TradeState.IN_POSITION:
            raise ValueError(f"close_order_submitted only from IN_POSITION, not {self.state}")
        self.close_order_id = close_order_id
        self.state = TradeState.CLOSING
        self.extra_data["close_reason"] = reason or "user_close"

    def close_filled(self) -> None:
        """Close order filled → trade fully closed (stays in CLOSING, terminal)."""
        if self.state != TradeState.CLOSING:
            raise ValueError(f"close_filled only from CLOSING, not {self.state}")
        self.extra_data["close_filled"] = True

    def close_rejected(self) -> None:
        """Close order rejected → trade still considered closed for now."""
        if self.state != TradeState.CLOSING:
            raise ValueError(f"close_rejected only from CLOSING, not {self.state}")
        self.extra_data["close_rejected"] = True

    def force_cancel(self, reason: str = "forced") -> None:
        """Emergency cancel from any non‑terminal state."""
        if self.state in self.TERMINAL_STATES:
            return
        self.state = TradeState.CANCELING
        self.extra_data["cancel_reason"] = reason

    # ---------- Query helpers ----------
    def is_active(self) -> bool:
        return self.state not in self.TERMINAL_STATES

    def is_open(self) -> bool:
        return self.state == TradeState.IN_POSITION

    def is_terminal(self) -> bool:
        return self.state in self.TERMINAL_STATES

    def needs_validation(self) -> bool:
        return self.state == TradeState.VALIDATING

    def needs_protection(self) -> bool:
        return self.state == TradeState.PROTECTING

    def needs_entry_fill(self) -> bool:
        return self.state == TradeState.AWAITING_FILL

    def __repr__(self) -> str:
        return f"<OrderManagementSM state={self.state} trade={self.trade_id}>"


# ----------------------------------------------------------------------
# Strategy Config
# ----------------------------------------------------------------------

class TradeStrategyConfig(StrategyConfig, frozen=True, kw_only=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_id: str
    side: str
    size: float = 0.001
    entry_price: Optional[float] = None
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    strategy_id: Optional[str] = None
    # Which execution path this trade actually runs on — 'virtual' (sandbox
    # exec node) or 'real' (live Binance exec node). Required (no default)
    # so a trade can never be silently logged as the wrong one; this is what
    # gets written to trade_events.target for the audit trail, and it's also
    # how EDGETrader decides which of the two TradingNode instances to
    # attach this strategy to.
    target: str

# ----------------------------------------------------------------------
# Per‑Trade Strategy
# ----------------------------------------------------------------------

class TradeStrategy(Strategy):
    """
    One strategy instance per open trade.
    Owns an OrderManagementSM and reacts to order events.
    """

    def __init__(
        self,
        config: TradeStrategyConfig,
        close_callback: Optional[Callable[[str], None]] = None
    ):
        super().__init__(config)
        self.sm = OrderManagementSM()
        self._db = None  # will be lazily initialised
        self._close_callback = close_callback

        # Deterministic client_order_ids, one per purpose, all derived from
        # trade_id. Deterministic (not random) so that if the strategy
        # process crashes and restarts, it recomputes the same ids and
        # Binance's own dedup prevents double submission. The suffix
        # disambiguates entry vs. protection vs. close, since a single trade
        # now involves multiple orders over its lifetime, not just one.
        self._entry_client_order_id = f"edge-{config.trade_id}-entry"
        self._sl_client_order_id = f"edge-{config.trade_id}-sl"
        self._tp_client_order_id = f"edge-{config.trade_id}-tp"
        self._close_client_order_id = f"edge-{config.trade_id}-close"

        # Tracks SL/TP client_order_ids submitted-but-not-yet-accepted, so we
        # know when to fire protection_placed() (i.e. once ALL protection
        # orders the venue has confirmed, not just the first one).
        self._pending_protection_ids: set[str] = set()

        # Event-sourcing writes are fire-and-forget tasks. The lock (asyncio
        # locks are FIFO) makes them hit Postgres in the exact order they were
        # emitted, so previous_event_id links and occurred_at ordering are
        # correct. The set keeps task references alive until they finish.
        self._db_lock = asyncio.Lock()
        self._db_tasks: set[asyncio.Task] = set()

        # Externally requested cancel / exit (edge-api, scanner signal, ...).
        # Remembered until the trade can act on it, because a request can arrive
        # in any state (e.g. while the entry is still being accepted, or between
        # the entry fill and the SL/TP being live).
        self._exit_request: Optional[tuple[Optional[str], Optional[Dict[str, Any]]]] = None  # (reason, details)
        self._entry_cancel_sent = False
        self._close_reason: Optional[str] = None      # set once a market exit has been decided
        self._close_submitted = False
        self._awaiting_protection_cancel: set[str] = set()

    async def _get_db(self):
        """Lazy load the DB instance (avoids early import issues)."""
        if self._db is None:
            # Import here to avoid circular dependency
            from trades_db_async import TradeEventsDB
            self._db = await TradeEventsDB.get_instance()
        return self._db

    # ---------- DB helpers (event sourcing) ----------
    #
    # Every meaningful step of a trade is appended to trade_events as its own
    # immutable row. Event types written by this strategy (all <= 20 chars):
    #
    #   EntrySubmitted    entry order handed to the venue gateway
    #   Opened            venue accepted the entry order (trade is live)
    #   Filled            entry order filled
    #   SLPlaced          stop-loss order accepted by the venue
    #   TPPlaced          take-profit order accepted by the venue
    #   SLHit / TPHit     a protection order filled (position exited)
    #   SiblingCancelSent cancel sent for the other protection order (manual OCO)
    #   CancelRequested   cancel asked for (edge-api / cancel_requests table)
    #   CloseRequested    market exit order submitted (manual or signal-driven)
    #   CloseFilled       market exit order filled (carries qty / commission)
    #   ProtectionCancelSent  cancel sent for a resting SL/TP before a market exit
    #   OrderRejected     venue rejected an order (any role)
    #   OrderDenied       Nautilus risk engine denied an order (any role)
    #   Closed/Cancelled  terminal event, carries close_reason / cancel_reason
    #
    # Per-event details (order role, client/venue order ids, qty, commission,
    # exchange timestamp...) go in the `metadata` JSONB column.

    def _order_role(self, coid: str) -> str:
        if coid == self._entry_client_order_id:
            return "entry"
        if coid == self._sl_client_order_id:
            return "sl"
        if coid == self._tp_client_order_id:
            return "tp"
        if coid == self._close_client_order_id:
            return "close"
        return "unknown"

    def _order_meta(self, event, role: str) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "order_role": role,
            "client_order_id": str(event.client_order_id),
        }
        venue_oid = getattr(event, "venue_order_id", None)
        if venue_oid is not None:
            meta["venue_order_id"] = str(venue_oid)
        ts_event = getattr(event, "ts_event", None)
        if ts_event:
            meta["exchange_ts_ns"] = int(ts_event)
        return meta

    def _fill_meta(self, event: OrderFilled, role: str) -> Dict[str, Any]:
        meta = self._order_meta(event, role)
        meta["last_qty"] = float(event.last_qty)
        meta["last_px"] = float(event.last_px)
        meta["exchange_trade_id"] = str(event.trade_id)
        liq = getattr(event, "liquidity_side", None)
        if liq is not None:
            meta["liquidity_side"] = getattr(liq, "name", str(liq))
        commission = getattr(event, "commission", None)
        if commission is not None:
            meta["commission"] = str(commission)
        return meta

    def _emit(
        self,
        event_type: str,
        *,
        fill_price: Optional[float] = None,
        close_reason: Optional[str] = None,
        cancel_reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Queue one event for this trade (non-blocking, order-preserving)."""
        task = asyncio.create_task(
            self._append_event(event_type, fill_price, close_reason, cancel_reason, metadata or {})
        )
        self._db_tasks.add(task)
        task.add_done_callback(self._db_tasks.discard)

    async def _append_event(
        self,
        event_type: str,
        fill_price: Optional[float],
        close_reason: Optional[str],
        cancel_reason: Optional[str],
        metadata: Dict[str, Any],
    ) -> None:
        db_for_notify = None
        async with self._db_lock:
            try:
                db = await self._get_db()
                db_for_notify = db
                await db.insert_trade_event(
                    trade_id=self.config.trade_id,
                    event_type=event_type,
                    instrument=str(self.config.instrument_id),
                    side=self.config.side,
                    size=self.config.size,
                    occurred_at=None,  # DB now(); exchange time is kept in metadata
                    ep=self.config.entry_price,
                    sl=self.config.sl_price,
                    tp=self.config.tp_price,
                    fill_price=fill_price,
                    close_reason=close_reason,
                    cancel_reason=cancel_reason,
                    metadata=metadata,
                    target=self.config.target,
                )
                self.log.info(
                    f"📝 Logged {event_type} event for {self.config.trade_id} (target={self.config.target})"
                )
            except Exception as e:
                self.log.error(f"❌ Failed to log {event_type} event for {self.config.trade_id}: {e}")


        # Telegram / Google Sheets: outside the DB lock and fully isolated, so a
        # slow or failing notification can never delay or break event logging.
        # Sent even if the DB write failed (that is when you most want to know).
        try:
            from trade_notifier import get_notifier
            get_notifier().submit(db_for_notify, {
                "trade_id": self.config.trade_id,
                "event_type": event_type,
                "instrument": str(self.config.instrument_id),
                "side": self.config.side,
                "size": self.config.size,
                "ep": self.config.entry_price,
                "sl": self.config.sl_price,
                "tp": self.config.tp_price,
                "fill_price": fill_price,
                "close_reason": close_reason,
                "cancel_reason": cancel_reason,
                "metadata": metadata,
                "target": self.config.target,
            })
        except Exception as e:
            self.log.error(f"❌ Failed to queue notification for {event_type}: {e}")

    def _log_terminal_event(self) -> None:
        """Emit the final 'Closed' or 'Cancelled' event from the SM's state."""
        if self.sm.state == TradeState.CLOSING:
            self._emit(
                "Closed",
                fill_price=self.sm.extra_data.get("fill_price"),
                close_reason=self.sm.extra_data.get("close_reason", "protection_filled"),
            )
        elif self.sm.state == TradeState.CANCELING:
            self._emit(
                "Cancelled",
                fill_price=self.sm.extra_data.get("fill_price"),  # may be None
                cancel_reason=self.sm.extra_data.get("cancel_reason", "forced"),
            )
        else:
            self.log.warning(f"Attempted to append closing event while in {self.sm.state}")

    # -------- Modified _finalize_and_stop --------
    def _finalize_and_stop(self) -> None:
        """Terminal state reached: log event, then either hand off to the
        listener (which deregisters us — and, per node.trader.remove_strategy,
        already stops us as part of that) or stop directly if there's no
        listener. Calling both would double-stop an already-STOPPED strategy
        and raise InvalidStateTrigger('STOPPED -> STOP')."""
        # Fire‑and‑forget the DB insert (we don't wait for it)
        self._log_terminal_event()

        self.log.info(
            f"Trade {self.config.trade_id} reached terminal state {self.sm.state}; stopping"
        )

        if self._close_callback is not None:
            # The callback (EDGETrader's on_strategy_closed) deregisters this
            # strategy via node.trader.remove_strategy(), which already stops
            # it internally — don't stop it again here.
            self._close_callback(self.config.trade_id)
        else:
            # No listener wired up (e.g. standalone/unit-test usage) — we're
            # responsible for stopping ourselves.
            self.stop()

    # ---------- Lifecycle ----------
    def on_start(self) -> None:
        """Start the strategy: open the trade and submit the entry order."""
        self.log.info(
            f"🚀 TradeStrategy started for {self.config.trade_id} "
            f"(instrument={self.config.instrument_id})"
        )

        # TODO: Real validation logic (size/price sanity, risk limits, etc.)
        # goes here. For now we assume it always passes.
        self.sm.validate_passed(self.config.trade_id, self._entry_client_order_id)
        # On failure this would instead be:
        #     self.sm.validate_failed(reason="...")
        #     self.stop()
        #     return

        # Subscribe to bars (needed for price updates or if we want to monitor)
        self.subscribe_bars(self.config.bar_type)

        # The sandbox exchange only receives topics matching data.*.BINANCE.*
        # (quotes/trades/book). Bar topics don't match, so bars alone can never fill orders.
        if self.config.target == "virtual":
            self.subscribe_trade_ticks(self.config.instrument_id)
            self.subscribe_quote_ticks(self.config.instrument_id)

        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            # Can't size/price an order without instrument metadata (precision,
            # increments, etc). Treat like a failed validation.
            self.log.error(
                f"No instrument found in cache for {self.config.instrument_id}; "
                f"cannot submit entry order for {self.config.trade_id}"
            )
            self.sm.force_cancel(reason="instrument_not_found")
            # Use the normal terminal-state path (not a bare self.stop()) so this
            # failure gets: (1) a Cancelled event logged to trade_events for the
            # audit trail, and (2) the strategy properly deregistered from the
            # trader via the close_callback. Skipping this previously left a dead
            # strategy permanently registered with the trader, which then caused
            # InvalidStateTrigger errors the next time the trader tried to restart.
            self._finalize_and_stop()
            return

        side = OrderSide.BUY if self.config.side.upper() == "BUY" else OrderSide.SELL
        quantity = instrument.make_qty(self.config.size)

        if self.config.entry_price is not None:
            entry_order = self.order_factory.limit(
                instrument_id=self.config.instrument_id,
                order_side=side,
                quantity=quantity,
                price=instrument.make_price(self.config.entry_price),
                client_order_id=ClientOrderId(self._entry_client_order_id),
            )
        else:
            entry_order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=side,
                quantity=quantity,
                client_order_id=ClientOrderId(self._entry_client_order_id),
            )

        self.submit_order(entry_order)
        self.log.info(
            f"📈 Entry order submitted for {self.config.trade_id} "
            f"side={self.config.side} client_order_id={self._entry_client_order_id}"
        )

    # ---------- Order event handlers ----------
    #
    # NOTE: on_order_submitted() and on_order_accepted() are DIFFERENT events
    # in Nautilus — submitted fires when we hand the order to the venue
    # gateway, accepted fires when the venue confirms it's live. Our SM's
    # entry_accepted() transition belongs on acceptance, not submission.

    def on_order_submitted(self, event: OrderSubmitted) -> None:
        """Order handed off to the venue gateway. Confirmation comes later
        via on_order_accepted(); this is just a log/telemetry hook."""
        self.log.debug(f"Order submitted: {event.client_order_id}")
        if self._is_entry_order(event):
            self._emit("EntrySubmitted", metadata=self._order_meta(event, "entry"))

    def on_order_accepted(self, event: OrderAccepted) -> None:
        """Venue confirmed the order is live."""
        coid = str(event.client_order_id)

        if self._is_entry_order(event) and self.sm.state == TradeState.PLACING_ENTRY:
            self.sm.entry_accepted()
            self.log.info(f"✅ Entry order accepted, awaiting fill (trade={self.config.trade_id})")
            # Record the Opened event now, not just after fill, so the trade
            # is discoverable (e.g. by a cancel event) while AWAITING_FILL.
            self._emit("Opened", metadata=self._order_meta(event, "entry"))
            self._apply_exit_request()      # a cancel may have arrived while the entry was being accepted

        elif coid in self._pending_protection_ids:
            self._pending_protection_ids.discard(coid)
            role = self._order_role(coid)
            self._emit(
                "SLPlaced" if role == "sl" else "TPPlaced",
                metadata=self._order_meta(event, role),
            )
            if not self._pending_protection_ids and self.sm.needs_protection():
                self.sm.protection_placed()
                self.log.info(f"🛡️ Protection orders live for {self.config.trade_id}")
                self._apply_exit_request()  # an exit may have arrived while SL/TP were being placed

    def on_order_filled(self, event: OrderFilled) -> None:
        """Called when any order fills."""
        self.log.info(f"📊 Order filled: {event.client_order_id} @ {event.last_px}")

        # Store fill price for the closing event
        self.sm.extra_data["fill_price"] = float(event.last_px)

        if self._is_entry_order(event) and self.sm.state == TradeState.AWAITING_FILL:
            self.sm.entry_filled()
            # Record the Filled event → SM is moving into PROTECTING
            self._emit(
                "Filled",
                fill_price=float(event.last_px),
                metadata=self._fill_meta(event, "entry"),
            )
            self._submit_protection_orders()

        elif self._is_protection_order(event) and self.sm.state == TradeState.IN_POSITION:
            self.sm.protection_filled()
            role = self._order_role(str(event.client_order_id))
            hit = "sl_hit" if role == "sl" else "tp_hit"
            self.sm.extra_data["close_reason"] = hit  # was a generic "protection_filled"
            self._emit(
                "SLHit" if role == "sl" else "TPHit",
                fill_price=float(event.last_px),
                metadata=self._fill_meta(event, role),
            )
            self._cancel_sibling_protection_order(event)
            self._finalize_and_stop()

        elif self._is_close_order(event) and self.sm.state == TradeState.CLOSING:
            self.sm.close_filled()
            # Without this event the exit fill's qty / commission were never stored,
            # so trade_metrics could not price the exit fee.
            self._emit(
                "CloseFilled",
                fill_price=float(event.last_px),
                metadata=self._fill_meta(event, "close"),
            )
            self._finalize_and_stop()

        else:
            self.log.warning(
                f"Unexpected fill for {event.client_order_id} while "
                f"trade={self.config.trade_id} is in state {self.sm.state}"
            )

    def on_order_rejected(self, event: OrderRejected) -> None:
        """Called when an order is rejected."""
        self.log.warning(f"❌ Order rejected: {event.client_order_id} ({event.reason})")
        self._emit(
            "OrderRejected",
            metadata={**self._order_meta(event, self._order_role(str(event.client_order_id))),
                      "reason": str(event.reason)},
        )

        if self._is_entry_order(event):
            self.sm.entry_rejected(reason=event.reason)
            self._finalize_and_stop()
        else:
            # A protection or close order got rejected mid-flight. Rather
            # than leave the trade stuck, treat it as an emergency cancel —
            # a human/ops process should reconcile the resulting position
            # manually (this SM has no automatic retry logic yet).
            self.sm.force_cancel(reason=f"order_rejected:{event.client_order_id}")
            self._finalize_and_stop()

    def on_order_denied(self, event: OrderDenied) -> None:
        """Called when an order is denied by Nautilus's own risk engine
        before ever reaching the venue (e.g. NOTIONAL_LESS_THAN_MIN_FOR_INSTRUMENT).

        This is distinct from on_order_rejected, which fires for a
        venue-side rejection — OrderDenied never even leaves the client.
        Without this handler the strategy was left running forever (never
        reaching a terminal state, never removed from active_strategies),
        and no Opened/Cancelled event ever got logged for the trade, which
        left downstream Cancel messages for that ticker unable to find an
        active trade and retrying indefinitely. Treat it the same as a
        rejected entry order: cancel and finalize.
        """
        self.log.warning(f"🚫 Order denied: {event.client_order_id} ({event.reason})")
        self._emit(
            "OrderDenied",
            metadata={**self._order_meta(event, self._order_role(str(event.client_order_id))),
                      "reason": str(event.reason)},
        )

        if self._is_entry_order(event):
            self.sm.entry_rejected(reason=event.reason)
            self._finalize_and_stop()
        else:
            # A protection or close order got denied mid-flight — same
            # reasoning as the equivalent branch in on_order_rejected: treat
            # it as an emergency cancel rather than leaving the trade stuck.
            self.sm.force_cancel(reason=f"order_denied:{event.client_order_id}")
            self._finalize_and_stop()

    def on_order_canceled(self, event: OrderCanceled) -> None:
        """Called when an order is canceled."""
        self.log.warning(f"🚫 Order canceled: {event.client_order_id}")
        coid = str(event.client_order_id)

        if self._is_entry_order(event) and self.sm.state == TradeState.AWAITING_FILL:
            # If we asked for this cancel (manual / scanner signal) record WHY;
            # an exchange-side cancel keeps the legacy "entry_cancelled".
            reason = self._exit_request[0] if self._exit_request is not None else None
            self.sm.entry_cancelled(reason=reason)
            self._finalize_and_stop()

        elif coid in self._awaiting_protection_cancel:
            self._protection_cancel_done(coid)

    def on_order_cancel_rejected(self, event: OrderCancelRejected) -> None:
        """The venue refused a cancel. For a resting SL/TP this usually means it
        just filled (the fill event settles the trade); we deliberately do NOT
        send the market exit here, that could double-close. If nothing settles
        it, the timeout in request_close() takes over."""
        coid = str(event.client_order_id)
        self.log.warning(f"❌ Cancel rejected: {coid} ({event.reason})")
        if coid in self._awaiting_protection_cancel:
            self._emit(
                "OrderRejected",
                metadata={**self._order_meta(event, self._order_role(coid)),
                          "reason": f"cancel rejected: {event.reason}"},
            )

    # ---------- Order construction helpers ----------

    def _submit_protection_orders(self) -> None:
        """Called once the entry order fills. Submits SL (stop-market) and/or
        TP (limit) as reduce-only orders, then waits for both to be accepted
        before driving the SM to IN_POSITION (see protection_placed())."""
        instrument = self.cache.instrument(self.config.instrument_id)
        exit_side = OrderSide.SELL if self.config.side.upper() == "BUY" else OrderSide.BUY
        quantity = instrument.make_qty(self.config.size)

        pending = {}
        if self.config.sl_price is not None:
            sl_order = self.order_factory.stop_market(
                instrument_id=self.config.instrument_id,
                order_side=exit_side,
                quantity=quantity,
                trigger_price=instrument.make_price(self.config.sl_price),
                reduce_only=True,
                client_order_id=ClientOrderId(self._sl_client_order_id),
            )
            pending[str(sl_order.client_order_id)] = sl_order

        if self.config.tp_price is not None:
            tp_order = self.order_factory.limit(
                instrument_id=self.config.instrument_id,
                order_side=exit_side,
                quantity=quantity,
                price=instrument.make_price(self.config.tp_price),
                reduce_only=True,
                client_order_id=ClientOrderId(self._tp_client_order_id),
            )
            pending[str(tp_order.client_order_id)] = tp_order

        if not pending:
            # No SL/TP configured for this trade — nothing to wait on.
            self.log.warning(
                f"No sl_price/tp_price set for {self.config.trade_id}; "
                f"skipping protection and moving straight to IN_POSITION"
            )
            self.sm.protection_placed()
            self._apply_exit_request()
            return

        self._pending_protection_ids = set(pending.keys())
        for order in pending.values():
            self.submit_order(order)

    def _cancel_sibling_protection_order(self, filled_event: OrderFilled) -> None:
        """Cancel whichever SL/TP order did NOT just fill.

        Binance's execution client denies any Nautilus order list containing
        linked orders (UNSUPPORTED_OCO_CONDITIONAL_ORDERS) — true exchange-side
        OCO isn't reachable through the standard submit path in this adapter
        version. SL and TP are therefore two independent orders, and we have
        to emulate one-cancels-other by hand here rather than relying on the
        venue to do it.
        """
        filled_coid = str(filled_event.client_order_id)
        sibling_coid = (
            self._tp_client_order_id
            if filled_coid == self._sl_client_order_id
            else self._sl_client_order_id
        )

        sibling_order = self.cache.order(ClientOrderId(sibling_coid))
        if sibling_order is None:
            # Sibling was never placed — e.g. only sl_price OR tp_price was
            # configured for this trade, not both.
            return

        if sibling_order.is_open:
            self.log.info(
                f"Cancelling sibling protection order {sibling_coid} "
                f"(filled: {filled_coid})"
            )
            self.cancel_order(sibling_order)
            self._emit(
                "SiblingCancelSent",
                metadata={
                    "order_role": "tp" if sibling_coid == self._tp_client_order_id else "sl",
                    "client_order_id": sibling_coid,
                },
            )
        else:
            self.log.debug(
                f"Sibling protection order {sibling_coid} already inactive "
                f"(status={sibling_order.status})"
            )

    # ---------- Exit / cancel requests ----------

    def request_close(self, reason: str = "user_close") -> None:
        """Exit an open position at market; `reason` becomes close_reason.

        Order matters (on spot, resting SL/TP sell orders lock the coins, and a
        protection order could fill at the same moment as the exit):
          1. cancel whichever SL/TP orders are still resting;
          2. only after the venue confirmed those cancels, submit the reduce-only
             market order (see _protection_cancel_done).
        If an SL/TP fills in between, the normal SLHit/TPHit path wins and no
        market order is ever sent.
        """
        if not self.sm.is_open():
            self.log.warning(
                f"Cannot close {self.config.trade_id}: SM not IN_POSITION "
                f"(state={self.sm.state})"
            )
            return
        if self._close_reason is not None:
            self.log.info(f"Exit already in progress for {self.config.trade_id}")
            return
        self._close_reason = reason

        resting = []
        for coid in (self._sl_client_order_id, self._tp_client_order_id):
            order = self.cache.order(ClientOrderId(coid))
            if order is not None and order.is_open:
                resting.append(order)

        if not resting:
            self._submit_close_order()
            return

        self._awaiting_protection_cancel = {str(o.client_order_id) for o in resting}
        for order in resting:
            coid = str(order.client_order_id)
            self.log.info(f"Cancelling {coid} before market exit of {self.config.trade_id} ({reason})")
            self.cancel_order(order)
            self._emit(
                "ProtectionCancelSent",
                metadata={"order_role": self._order_role(coid), "client_order_id": coid, "reason": reason},
            )
        asyncio.get_running_loop().call_later(PROTECTION_CANCEL_TIMEOUT_S, self._on_protection_cancel_timeout)

    def _protection_cancel_done(self, coid: str) -> None:
        """One resting SL/TP is gone; when all are, send the market exit — unless
        the position was already closed by an SL/TP fill in the meantime."""
        self._awaiting_protection_cancel.discard(coid)
        if self._awaiting_protection_cancel:
            return
        if self.sm.is_open() and not self._close_submitted:
            self._submit_close_order()

    def _on_protection_cancel_timeout(self) -> None:
        if self._awaiting_protection_cancel and self.sm.is_open() and not self._close_submitted:
            self.log.warning(
                f"Protection cancel not confirmed after {PROTECTION_CANCEL_TIMEOUT_S:.0f}s for "
                f"{self.config.trade_id} ({sorted(self._awaiting_protection_cancel)}); sending the exit anyway"
            )
            self._awaiting_protection_cancel.clear()
            try:
                self._submit_close_order()
            except Exception as e:
                self.log.error(f"❌ Could not submit the exit for {self.config.trade_id}: {e}")

    def _submit_close_order(self) -> None:
        """Submit the reduce-only market exit and move the SM to CLOSING."""
        if self._close_submitted or not self.sm.is_open():
            return
        self._close_submitted = True
        reason = self._close_reason or "user_close"

        instrument = self.cache.instrument(self.config.instrument_id)
        exit_side = OrderSide.SELL if self.config.side.upper() == "BUY" else OrderSide.BUY
        quantity = instrument.make_qty(self.config.size)

        close_order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=exit_side,
            quantity=quantity,
            reduce_only=True,
            client_order_id=ClientOrderId(self._close_client_order_id),
        )
        self._emit(
            "CloseRequested",
            metadata={
                "order_role": "close",
                "client_order_id": self._close_client_order_id,
                "close_reason": reason,
            },
        )
        self.sm.close_order_submitted(str(close_order.client_order_id), reason=reason)
        self.submit_order(close_order)

    def request_cancel(self, reason: Optional[str] = None, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Cancel the trade: entry still pending -> cancel the entry order;
        position open -> exit at market. The request is remembered and applied as
        soon as the trade can act on it, so it is not lost if it arrives while the
        entry is being accepted or the SL/TP are being placed.

        reason  -> trade_events.cancel_reason / close_reason of the final event
                   (None keeps the legacy values: "entry_cancelled" / "user_close").
        details -> free-form JSON stored with the CancelRequested event (e.g. the
                   scanner signal that triggered this).
        """
        if self.sm.is_terminal():
            self.log.warning(f"Cannot cancel trade in state {self.sm.state}")
            return
        if self._exit_request is not None:
            self.log.info(f"Cancel/exit already requested for {self.config.trade_id}")
            return

        self._exit_request = (reason, details)
        meta: Dict[str, Any] = {"state": self.sm.state.name, "reason": reason or "user_cancel"}
        if details:
            meta["details"] = details
        self._emit("CancelRequested", metadata=meta)
        self._apply_exit_request()

    def _apply_exit_request(self) -> None:
        """Act on a pending request if the trade is in a state where it can be
        executed. Called when a request arrives and again whenever the SM reaches
        AWAITING_FILL or IN_POSITION; in any other state it just waits."""
        if self._exit_request is None:
            return
        reason, _details = self._exit_request

        if self.sm.state == TradeState.AWAITING_FILL:
            if self._entry_cancel_sent:
                return
            entry_order = self.cache.order(ClientOrderId(self._entry_client_order_id))
            if entry_order is not None and entry_order.is_open:
                self._entry_cancel_sent = True
                self.cancel_order(entry_order)
                self.log.info(f"Cancelling entry order for {self.config.trade_id}" + (f" ({reason})" if reason else ""))
            else:
                self.log.warning(f"Entry order not open; state {self.sm.state}")

        elif self.sm.state == TradeState.IN_POSITION:
            self.request_close(reason=reason or "user_close")

    def _is_entry_order(self, event) -> bool:
        return str(event.client_order_id) == self._entry_client_order_id

    def _is_protection_order(self, event) -> bool:
        coid = str(event.client_order_id)
        return coid in (self._sl_client_order_id, self._tp_client_order_id)

    def _is_close_order(self, event) -> bool:
        return str(event.client_order_id) == self._close_client_order_id

    # ---------- Bar handler (optional) ----------
    def on_bar(self, bar: Bar) -> None:
        """Called when a new bar arrives; can be used for monitoring."""
        # We might not need this for the MVP, but we keep the subscription.
        pass

    def on_stop(self) -> None:
        """Stop the strategy: clean up resources."""
        self.log.info(f"🛑 TradeStrategy stopped for {self.config.trade_id}")
        # Optionally append a final event if not already done
