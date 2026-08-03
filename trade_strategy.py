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
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

# We'll import the DB lazily inside the strategy to avoid circular imports
# from trades_db_async import TradeEventsDB


# ----------------------------------------------------------------------
# State Machine
# ----------------------------------------------------------------------

class TradeState(Enum):
    """States of a single trade lifecycle."""
    IDLE = auto()
    PLACING_ENTRY = auto()
    AWAITING_FILL = auto()
    PROTECTING = auto()
    IN_POSITION = auto()
    CLOSING = auto()
    CLOSED = auto()
    CANCELLED = auto()


class OrderManagementSM:
    """
    Pure state machine for one trade.
    No external dependencies – tested in isolation.
    """

    def __init__(self):
        self.state = TradeState.IDLE
        self.trade_id: Optional[str] = None
        self.entry_order_id: Optional[str] = None
        self.close_order_id: Optional[str] = None
        self.extra_data: Dict[str, Any] = {}

    # ---------- Transitions ----------

    def open_trade(self, trade_id: str, entry_order_id: str) -> None:
        """Start a new trade: submit entry order."""
        if self.state != TradeState.IDLE:
            raise ValueError(f"Cannot open trade from {self.state}")
        self.trade_id = trade_id
        self.entry_order_id = entry_order_id
        self.state = TradeState.PLACING_ENTRY

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
        self.state = TradeState.CANCELLED
        self.extra_data["cancel_reason"] = reason or "entry_rejected"

    def entry_cancelled(self) -> None:
        """Entry order cancelled (e.g. user request) → cancelled."""
        if self.state != TradeState.AWAITING_FILL:
            raise ValueError(f"entry_cancelled only from AWAITING_FILL, not {self.state}")
        self.state = TradeState.CANCELLED
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
        self.state = TradeState.CLOSED

    def close_order_submitted(self, close_order_id: str) -> None:
        """User‑initiated close: submit market/limit order."""
        if self.state != TradeState.IN_POSITION:
            raise ValueError(f"close_order_submitted only from IN_POSITION, not {self.state}")
        self.close_order_id = close_order_id
        self.state = TradeState.CLOSING

    def close_filled(self) -> None:
        """Close order filled → trade closed."""
        if self.state != TradeState.CLOSING:
            raise ValueError(f"close_filled only from CLOSING, not {self.state}")
        self.state = TradeState.CLOSED

    def close_rejected(self) -> None:
        """Close order rejected → for now mark as closed."""
        if self.state != TradeState.CLOSING:
            raise ValueError(f"close_rejected only from CLOSING, not {self.state}")
        self.state = TradeState.CLOSED

    def force_cancel(self, reason: str = "forced") -> None:
        """Emergency cancel from any state (except terminal)."""
        if self.state in (TradeState.CLOSED, TradeState.CANCELLED):
            return
        self.state = TradeState.CANCELLED
        self.extra_data["cancel_reason"] = reason

    # ---------- Query helpers ----------
    def is_active(self) -> bool:
        return self.state not in (TradeState.CLOSED, TradeState.CANCELLED)

    def is_open(self) -> bool:
        return self.state == TradeState.IN_POSITION

    def is_terminal(self) -> bool:
        return self.state in (TradeState.CLOSED, TradeState.CANCELLED)

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
    trade_id: str                     # same as order_id_tag
    order_id_tag: str                 # deterministic prefix for client_order_id
    size: float = 0.001               # default position size
    entry_price: Optional[float] = None
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    # Additional fields can be added as needed (e.g., side)


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
        # deterministic client_order_id: "{tag}-{trade_id}"
        # This helps Binance dedupe and makes traceability easier.
        self._client_order_id = f"{config.order_id_tag}-{config.trade_id}"

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

        # Advance SM
        self.sm.open_trade(self.config.trade_id, self._client_order_id)

        # Subscribe to bars (needed for price updates or if we want to monitor)
        self.subscribe_bars(self.config.bar_type)

        # TODO: Submit the entry order using self.submit_order()
        # The order should use the deterministic client_order_id.
        # We'll implement this in a later step when we wire everything.
        self.log.info(
            f"📈 Entry order for {self.config.trade_id} would be submitted "
            f"with client_order_id={self._client_order_id} (not implemented yet)"
        )

    # ---------- Order event handlers ----------

    def on_order_filled(self, order) -> None:
        """Called when any order fills."""
        # We need to differentiate entry vs close vs protection orders.
        # We'll add logic later based on order.client_order_id or tags.
        self.log.info(f"📊 Order filled: {order}")

        # TODO: Drive SM based on which order filled
        # Example:
        # if self._is_entry_order(order):
        #     self.sm.entry_filled()
        #     # then place protection orders
        # elif self._is_close_order(order):
        #     self.sm.close_filled()
        #     # then append final event to DB and stop strategy

    def on_order_rejected(self, order) -> None:
        """Called when an order is rejected."""
        self.log.warning(f"❌ Order rejected: {order}")

        # Determine which order and drive SM accordingly
        # if self._is_entry_order(order):
        #     self.sm.entry_rejected(reason=order.reject_reason)
        #     # then stop strategy and append Cancelled event

    def on_order_canceled(self, order) -> None:
        """Called when an order is canceled."""
        self.log.warning(f"🚫 Order canceled: {order}")

        # if self._is_entry_order(order):
        #     self.sm.entry_cancelled()
        #     # then stop strategy

    def on_order_submitted(self, order) -> None:
        """Called when an order is submitted successfully (accepted)."""
        # For entry order acceptance, we can transition to AWAITING_FILL
        # but Binance may already send on_order_filled if it fills immediately.
        # We'll handle it when we implement the order submission.
        pass

    # ---------- Bar handler (optional) ----------
    def on_bar(self, bar: Bar) -> None:
        """Called when a new bar arrives; can be used for monitoring."""
        # We might not need this for the MVP, but we keep the subscription.
        pass

    def on_stop(self) -> None:
        """Stop the strategy: clean up resources."""
        self.log.info(f"🛑 TradeStrategy stopped for {self.config.trade_id}")
        # Optionally append a final event if not already done