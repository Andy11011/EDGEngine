"""Per‑trade strategy and state machine for event‑driven execution.

This module defines:
- `OrderManagementSM`: a pure state machine for a single trade.
- `TradeStrategyConfig`: configuration for a per‑trade strategy instance.
- `TradeStrategy`: a Nautilus `Strategy` subclass that owns one SM instance.

All classes are additive and not yet wired into `EDGETrader.py`.
"""

from __future__ import annotations

from enum import Enum, auto
from typing import Optional, Dict, Any

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events import (
    OrderAccepted,
    OrderCanceled,
    OrderFilled,
    OrderRejected,
    OrderSubmitted,
)
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from nautilus_trader.trading.strategy import Strategy

# We'll import the DB lazily inside the strategy to avoid circular imports
# from trades_db_async import TradeEventsDB


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

    def entry_cancelled(self) -> None:
        """Entry order cancelled (e.g. user request) → cancelled."""
        if self.state != TradeState.AWAITING_FILL:
            raise ValueError(f"entry_cancelled only from AWAITING_FILL, not {self.state}")
        self.state = TradeState.CANCELING
        self.extra_data["cancel_reason"] = "entry_cancelled"

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

    def close_order_submitted(self, close_order_id: str) -> None:
        """User‑initiated close: submit market/limit order."""
        if self.state != TradeState.IN_POSITION:
            raise ValueError(f"close_order_submitted only from IN_POSITION, not {self.state}")
        self.close_order_id = close_order_id
        self.state = TradeState.CLOSING
        self.extra_data["close_reason"] = "user_close"

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

class TradeStrategyConfig(StrategyConfig, frozen=True):
    """Configuration for a single‑trade strategy instance."""
    instrument_id: InstrumentId
    bar_type: BarType
    trade_id: str                     # unique per trade; also used as Nautilus order_id_tag
    side: str                         # "BUY" or "SELL" — matches trade_events.side in the DB
    size: float = 0.001               # default position size
    entry_price: Optional[float] = None  # None → market entry; set → limit entry
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None


# ----------------------------------------------------------------------
# Per‑Trade Strategy
# ----------------------------------------------------------------------

class TradeStrategy(Strategy):
    """
    One strategy instance per open trade.
    Owns an OrderManagementSM and reacts to order events.
    """

    def __init__(self, config: TradeStrategyConfig):
        super().__init__(config)
        self.sm = OrderManagementSM()
        self._db = None  # will be lazily initialised

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

    async def _get_db(self):
        """Lazy load the DB instance (avoids early import issues)."""
        if self._db is None:
            # Import here to avoid circular dependency
            from trades_db_async import TradeEventsDB
            self._db = await TradeEventsDB.get_instance()
        return self._db

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

        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            # Can't size/price an order without instrument metadata (precision,
            # increments, etc). Treat like a failed validation.
            self.log.error(
                f"No instrument found in cache for {self.config.instrument_id}; "
                f"cannot submit entry order for {self.config.trade_id}"
            )
            self.sm.force_cancel(reason="instrument_not_found")
            self.stop()
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

    def on_order_accepted(self, event: OrderAccepted) -> None:
        """Venue confirmed the order is live."""
        coid = str(event.client_order_id)

        if self._is_entry_order(event) and self.sm.state == TradeState.PLACING_ENTRY:
            self.sm.entry_accepted()
            self.log.info(f"✅ Entry order accepted, awaiting fill (trade={self.config.trade_id})")

        elif coid in self._pending_protection_ids:
            self._pending_protection_ids.discard(coid)
            if not self._pending_protection_ids and self.sm.needs_protection():
                self.sm.protection_placed()
                self.log.info(f"🛡️ Protection orders live for {self.config.trade_id}")

    def on_order_filled(self, event: OrderFilled) -> None:
        """Called when any order fills."""
        self.log.info(f"📊 Order filled: {event.client_order_id} @ {event.last_px}")

        if self._is_entry_order(event) and self.sm.state == TradeState.AWAITING_FILL:
            self.sm.entry_filled()
            self._submit_protection_orders()

        elif self._is_protection_order(event) and self.sm.state == TradeState.IN_POSITION:
            self.sm.protection_filled()
            self._cancel_sibling_protection_order(event)
            self._finalize_and_stop()

        elif self._is_close_order(event) and self.sm.state == TradeState.CLOSING:
            self.sm.close_filled()
            self._finalize_and_stop()

        else:
            self.log.warning(
                f"Unexpected fill for {event.client_order_id} while "
                f"trade={self.config.trade_id} is in state {self.sm.state}"
            )

    def on_order_rejected(self, event: OrderRejected) -> None:
        """Called when an order is rejected."""
        self.log.warning(f"❌ Order rejected: {event.client_order_id} ({event.reason})")

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

    def on_order_canceled(self, event: OrderCanceled) -> None:
        """Called when an order is canceled."""
        self.log.warning(f"🚫 Order canceled: {event.client_order_id}")

        if self._is_entry_order(event) and self.sm.state == TradeState.AWAITING_FILL:
            self.sm.entry_cancelled()
            self._finalize_and_stop()

    # ---------- Order construction helpers ----------

    def _submit_protection_orders(self) -> None:
        """Called once the entry order fills. Submits SL (stop-market) and/or
        TP (limit) as reduce-only orders, then waits for both to be accepted
        before driving the SM to IN_POSITION (see protection_placed())."""
        instrument = self.cache.instrument(self.config.instrument_id)
        exit_side = OrderSide.SELL if self.config.side.upper() == "BUY" else OrderSide.BUY
        quantity = instrument.make_qty(self.config.size)

        pending: Dict[str, Any] = {}

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
        else:
            self.log.debug(
                f"Sibling protection order {sibling_coid} already inactive "
                f"(status={sibling_order.status})"
            )

    def request_close(self) -> None:
        """User‑initiated close of an open position (e.g. manual override
        from outside the strategy). Submits a reduce-only market order.

        TODO: cancel the resting SL/TP orders before submitting this, to
        avoid a race where the protection order fills at the same moment
        this close order does (double-close / over-selling the position).
        """
        if not self.sm.is_open():
            self.log.warning(
                f"Cannot close {self.config.trade_id}: SM not IN_POSITION "
                f"(state={self.sm.state})"
            )
            return

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
        self.sm.close_order_submitted(str(close_order.client_order_id))
        self.submit_order(close_order)

    # ---------- Order identity helpers ----------

    def _is_entry_order(self, event) -> bool:
        return str(event.client_order_id) == self._entry_client_order_id

    def _is_protection_order(self, event) -> bool:
        coid = str(event.client_order_id)
        return coid in (self._sl_client_order_id, self._tp_client_order_id)

    def _is_close_order(self, event) -> bool:
        return str(event.client_order_id) == self._close_client_order_id

    def _finalize_and_stop(self) -> None:
        """Trade has reached a terminal SM state (CLOSING or CANCELING).

        TODO: append the final trade_event to the DB here (via
        self._get_db()) before stopping, once the DB-write step is wired in.
        For now this just stops the strategy instance.
        """
        self.log.info(
            f"Trade {self.config.trade_id} reached terminal state "
            f"{self.sm.state}; stopping strategy"
        )
        self.stop()

    # ---------- Bar handler (optional) ----------
    def on_bar(self, bar: Bar) -> None:
        """Called when a new bar arrives; can be used for monitoring."""
        # We might not need this for the MVP, but we keep the subscription.
        pass

    def on_stop(self) -> None:
        """Stop the strategy: clean up resources."""
        self.log.info(f"🛑 TradeStrategy stopped for {self.config.trade_id}")
        # Optionally append a final event if not already done
