"""
telegram_messages.py — ALL Telegram wording lives here. Edit this file only.

How it works
------------
* ACTION_TEMPLATES: one message per state-machine event (blue circle).
  Key = trade_events.event_type written by TradeStrategy. Set a value to None
  (or delete the key) to mute that message.
* RESULT_TEMPLATES: the final message when a trade ends (green / red / other).
* Templates are plain str.format() strings in Telegram HTML mode
  (<b>, <i>, <code>). Unknown placeholders render as "n/a", never crash.
* Every value is HTML-escaped before it is inserted.

Placeholders available in ACTION_TEMPLATES
------------------------------------------
{dot} {tag} {ticker} {side} {size} {ep} {sl} {tp} {fill_price} {role}
{qty} {commission} {reason} {trade_id} {state}

Extra placeholders in RESULT_TEMPLATES
--------------------------------------
{result_dot} {net_pnl} {gross_pnl} {roi} {fees} {duration} {close_reason}
{cancel_reason} {notes}

{tag} is "[REAL]" or "[VIRTUAL]". Numbers are pre-formatted (see _fmt_*).
"""

from __future__ import annotations

import html
from typing import Any, Dict, Optional

# ---- circles ---------------------------------------------------------------
DOT_ACTION = "🔵"
DOT_WIN = "🟢"
DOT_LOSS = "🔴"
DOT_NEUTRAL = "⚪"
DOT_WARN = "🟠"

# ---- one message per SM action (blue) -------------------------------------
ACTION_TEMPLATES: Dict[str, Optional[str]] = {
    "EntrySubmitted": "{dot} {tag} <b>{ticker}</b> {side}: entry order submitted (size {size}, ep {ep})",
    "Opened": "{dot} {tag} <b>{ticker}</b> {side}: entry order accepted, waiting for fill",
    "Filled": "{dot} {tag} <b>{ticker}</b> {side}: entry FILLED @ {fill_price} (qty {qty}, fee {commission})",
    "SLPlaced": "{dot} {tag} <b>{ticker}</b>: stop-loss placed @ {sl}",
    "TPPlaced": "{dot} {tag} <b>{ticker}</b>: take-profit placed @ {tp}",
    "SLHit": "{dot} {tag} <b>{ticker}</b>: stop-loss hit @ {fill_price}",
    "TPHit": "{dot} {tag} <b>{ticker}</b>: take-profit hit @ {fill_price}",
    "SiblingCancelSent": "{dot} {tag} <b>{ticker}</b>: cancelling the other protection order ({role})",
    "CancelRequested": "{dot} {tag} <b>{ticker}</b>: cancel requested (state: {state})",
    "CloseRequested": "{dot} {tag} <b>{ticker}</b>: manual close order submitted",
    "OrderRejected": "{dot} {tag} <b>{ticker}</b>: order REJECTED by venue ({role}): {reason}",
    "OrderDenied": "{dot} {tag} <b>{ticker}</b>: order DENIED by risk engine ({role}): {reason}",
}

# ---- final result ----------------------------------------------------------
RESULT_TEMPLATES: Dict[str, str] = {
    "win": (
        "{result_dot} {tag} <b>{ticker}</b> {side}: <b>WIN</b>\n"
        "PnL: <b>{net_pnl} USDT</b> (ROI {roi}%)\n"
        "Gross {gross_pnl} | Fees {fees} USDT | Held {duration}\n"
        "Exit: {close_reason}"
    ),
    "loss": (
        "{result_dot} {tag} <b>{ticker}</b> {side}: <b>LOSS</b>\n"
        "PnL: <b>{net_pnl} USDT</b> (ROI {roi}%)\n"
        "Gross {gross_pnl} | Fees {fees} USDT | Held {duration}\n"
        "Exit: {close_reason}"
    ),
    # Closed, but PnL could not be computed honestly (e.g. BNB fee w/o rate)
    "closed_unknown": (
        "{result_dot} {tag} <b>{ticker}</b> {side}: closed, PnL unavailable\n"
        "Gross {gross_pnl} | Held {duration} | Exit: {close_reason}\n"
        "{notes}"
    ),
    # Cancelled before any fill: we were never in the trade
    "cancelled_no_fill": "{result_dot} {tag} <b>{ticker}</b> {side}: cancelled, never filled ({cancel_reason})",
    # Cancelled AFTER the entry filled: exposure existed, check the account!
    "cancelled_after_fill": (
        "{result_dot} {tag} <b>{ticker}</b> {side}: cancelled AFTER entry fill ({cancel_reason})\n"
        "Check the position manually, PnL unknown."
    ),
    # Metrics/DB lookup failed
    "unavailable": "{result_dot} {tag} <b>{ticker}</b>: trade finished ({terminal}), result lookup failed",
}


# ---- formatting helpers ----------------------------------------------------
class _Safe(dict):
    def __missing__(self, key: str) -> str:
        return "n/a"


def _num(v: Any, digits: int = 4) -> str:
    if v is None:
        return "n/a"
    try:
        return f"{float(v):,.{digits}f}".rstrip("0").rstrip(".") if abs(float(v)) < 1000 else f"{float(v):,.2f}"
    except (TypeError, ValueError):
        return str(v)


def _signed(v: Any, digits: int = 2) -> str:
    if v is None:
        return "n/a"
    return f"{float(v):+,.{digits}f}"


def _duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "n/a"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}h {m}m" if h else (f"{m}m {sec}s" if m else f"{sec}s")


def _esc(v: Any) -> str:
    return html.escape("n/a" if v is None else str(v))


def _render(template: str, values: Dict[str, Any]) -> str:
    return template.format_map(_Safe({k: _esc(v) for k, v in values.items()}))


def _base(ev: Dict[str, Any]) -> Dict[str, Any]:
    meta = ev.get("metadata") or {}
    instrument = str(ev.get("instrument", ""))
    return {
        "tag": f"[{str(ev.get('target', '')).upper()}]",
        "ticker": instrument.rsplit(".", 1)[0],
        "side": str(ev.get("side", "")).upper(),
        "size": _num(ev.get("size"), 6),
        "ep": _num(ev.get("ep")),
        "sl": _num(ev.get("sl")),
        "tp": _num(ev.get("tp")),
        "fill_price": _num(ev.get("fill_price")),
        "trade_id": ev.get("trade_id"),
        "role": meta.get("order_role"),
        "qty": _num(meta.get("last_qty"), 6),
        "commission": meta.get("commission"),
        "reason": meta.get("reason"),
        "state": meta.get("state"),
    }


# ---- public API ------------------------------------------------------------
def render_action(ev: Dict[str, Any]) -> Optional[str]:
    """Blue-circle message for one SM event, or None if muted/unknown."""
    template = ACTION_TEMPLATES.get(ev.get("event_type"))
    if not template:
        return None
    return _render(template, {**_base(ev), "dot": DOT_ACTION})


def render_result(ev: Dict[str, Any], trade: Optional[Dict[str, Any]], metrics: Any) -> str:
    """Final message for a Closed/Cancelled terminal event."""
    values = _base(ev)
    terminal = ev.get("event_type")
    values.update(
        terminal=terminal,
        close_reason=ev.get("close_reason"),
        cancel_reason=ev.get("cancel_reason"),
        result_dot=DOT_NEUTRAL,
    )

    if terminal == "Cancelled":
        filled = bool(trade and trade.get("entry_fill_price") is not None)
        key = "cancelled_after_fill" if filled else "cancelled_no_fill"
        values["result_dot"] = DOT_WARN if filled else DOT_NEUTRAL
        return _render(RESULT_TEMPLATES[key], values)

    if metrics is None:
        return _render(RESULT_TEMPLATES["unavailable"], values)

    values.update(
        gross_pnl=_signed(metrics.gross_pnl),
        duration=_duration(metrics.duration_s),
        fees=_num(metrics.fee_usdt),
        notes="; ".join(metrics.notes) or "",
    )
    if metrics.net_pnl is None:
        return _render(RESULT_TEMPLATES["closed_unknown"], values)

    values.update(net_pnl=_signed(metrics.net_pnl), roi=_signed(metrics.roi_pct))
    if metrics.win:
        values["result_dot"] = DOT_WIN
        return _render(RESULT_TEMPLATES["win"], values)
    values["result_dot"] = DOT_LOSS
    return _render(RESULT_TEMPLATES["loss"], values)
