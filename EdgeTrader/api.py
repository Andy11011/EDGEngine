from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List, Optional   # added List

app = FastAPI()

# Shared mutable reference – will be assigned from EDGETrader
active_strategies: Dict[str, Any] = {}

class TradeStatus(BaseModel):
    trade_id: str
    state: str
    instrument: str
    side: str
    size: float
    entry_price: Optional[float]
    sl_price: Optional[float]
    tp_price: Optional[float]

@app.get("/health")
async def health_check():
    """
    Simple liveness probe.
    You can extend it later to check DB connectivity, node status, etc.
    """
    return {
        "status": "ok",
        "active_trades_count": len(active_strategies)
    }

@app.get("/active_trades", response_model=List[TradeStatus])
async def get_active_trades():
    result = []
    for trade_id, strategy in active_strategies.items():
        result.append(TradeStatus(
            trade_id=trade_id,
            state=strategy.sm.state.name,
            instrument=str(strategy.config.instrument_id),
            side=strategy.config.side,
            size=strategy.config.size,
            entry_price=strategy.config.entry_price,
            sl_price=strategy.config.sl_price,
            tp_price=strategy.config.tp_price,
        ))
    return result

@app.post("/cancel/{trade_id}")
async def cancel_trade(trade_id: str):
    strategy = active_strategies.get(trade_id)
    if not strategy:
        raise HTTPException(404, f"Trade {trade_id} not active")
    strategy.request_cancel()
    return {"status": "cancel_requested"}