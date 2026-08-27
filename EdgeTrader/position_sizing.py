"""
position_sizing.py — position-size calculation for opened trades.

Mirrors LevelsBot_v1_0_7.pine's entry_zone_calc_routine() exactly, so a
trade sized here matches what the Pine strategy would have produced if it
were sized off the same equity figure:

    riskAmount       = equity * RiskRatio
    risk             = abs(entryPrice - stopLossPrice)
    positionSize     = riskAmount / risk
    maxPositionSize  = 0.99 * (equity / entryPrice)
    size             = useMargin ? positionSize : min(maxPositionSize, positionSize)

We don't support the useMargin=true / leverage path — this is spot sizing
only, always capped at 99% of equity (same safety margin Pine leaves for
fees/slippage). If margin support is ever needed here, add a `use_margin`
flag and skip the min() the same way Pine does.

This module has no nautilus_trader or DB dependency on purpose — it's a
pure function, easy to unit test, and reused identically regardless of
where the equity figure came from (real account balance vs. virtual
balance from trades_config).
"""

from __future__ import annotations


class PositionSizingError(ValueError):
    """Raised when a position size can't be computed safely — e.g. equity
    isn't positive, or the stop-loss doesn't actually differ from entry.
    Callers should treat this as a hard stop (skip the trade), not
    something to silently work around."""


def calculate_position_size(
    equity: float,
    risk_ratio: float,
    entry_price: float,
    stop_loss_price: float,
) -> float:
    """
    Returns the position size (base-asset units) for a trade, given the
    equity to size against, the risk ratio (e.g. 0.001 for 0.1%), the
    entry price, and the stop-loss price.

    Raises PositionSizingError for degenerate inputs instead of returning
    inf/negative/NaN, so a bad config or payload fails loudly rather than
    submitting a nonsense order size.
    """
    if equity <= 0:
        raise PositionSizingError(f"equity must be positive, got {equity}")
    if risk_ratio <= 0:
        raise PositionSizingError(f"risk_ratio must be positive, got {risk_ratio}")
    if entry_price <= 0:
        raise PositionSizingError(f"entry_price must be positive, got {entry_price}")

    risk = abs(entry_price - stop_loss_price)
    if risk <= 0:
        raise PositionSizingError(
            f"stop_loss_price ({stop_loss_price}) must differ from "
            f"entry_price ({entry_price}) to compute a risk distance"
        )

    risk_amount = equity * risk_ratio
    position_size = risk_amount / risk
    max_position_size = 0.99 * (equity / entry_price)

    return min(position_size, max_position_size)
