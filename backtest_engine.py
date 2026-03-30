"""
backtest_engine.py
Historical backtesting with no look-ahead bias.

Entry at t+1 open, signal generated at t using only data up to t.
Exits:
  - stop loss / take profit using intraday bar high/low
  - opposite signal (exit at next open)
  - time exit
  - end of data

Fees and slippage are configurable (bps).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from market_data import fetch_indicator_frame
from signal_engine import build_quant_signal


def _clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))


def _safe_div(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b


def _compute_drawdown(equity: np.ndarray) -> float:
    peak = np.maximum.accumulate(equity)
    dd = (equity - peak) / peak
    return float(dd.min()) if dd.size else 0.0


def _sharpe(daily_returns: np.ndarray) -> Optional[float]:
    if daily_returns.size < 2:
        return None
    mu = float(np.nanmean(daily_returns))
    sd = float(np.nanstd(daily_returns))
    if sd == 0:
        return None
    return float((mu / sd) * np.sqrt(252))


def _cagr(initial: float, final: float, days: float) -> Optional[float]:
    if initial <= 0 or final <= 0 or days <= 0:
        return None
    return float((final / initial) ** (365.0 / days) - 1)


def run_backtest(
    symbol: str,
    asset_meta: Dict[str, Any],
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    period: str = "3y",
    interval: str = "1d",
    initial_capital: float = 10000.0,
    fees_bps: float = 5.0,
    slippage_bps: float = 2.0,
    max_holding_days: int = 10,
    allow_short: bool = True,
) -> Dict[str, Any]:
    df_ind = fetch_indicator_frame(symbol, start=start, end=end, period=period, interval=interval)
    if df_ind is None or df_ind.empty:
        return {
            "symbol": symbol,
            "has_real_data": False,
            "error": "NO_DATA (missing OHLCV/indicators)",
        }

    # Precompute signals for each bar (t) so that decisions at t are well-defined.
    # Entry uses t+1 open; stop/TP uses levels derived from indicators at t.
    signals: List[Dict[str, Any]] = []
    for i in range(len(df_ind)):
        sig = build_quant_signal(df_ind.iloc[i], asset_meta)
        signals.append(sig)

    fees_rate = float(fees_bps) / 10000.0
    slip_rate = float(slippage_bps) / 10000.0

    equity_curve = np.full(len(df_ind), np.nan, dtype=float)
    capital = float(initial_capital)
    peak_equity = capital

    trades: List[Dict[str, Any]] = []
    position: Optional[Dict[str, Any]] = None

    # Determine benchmark prices (buy and hold) at the first valid close.
    valid_start_idx = 0
    while valid_start_idx < len(df_ind) and pd.isna(df_ind["close"].iloc[valid_start_idx]):
        valid_start_idx += 1
    if valid_start_idx >= len(df_ind) - 1:
        valid_start_idx = 0
    bench_start_price = float(df_ind["close"].iloc[valid_start_idx])
    bench_end_price = float(df_ind["close"].iloc[-1])
    benchmark_equity_end = initial_capital * (bench_end_price / bench_start_price) if bench_start_price else initial_capital

    for t in range(len(df_ind) - 1):
        bar = df_ind.iloc[t]
        ts = df_ind.index[t]

        # Mark-to-market when in position.
        if position is not None:
            day_close = float(bar["close"])
            entry_price_adj = float(position["entry_price_adj"])
            direction = int(position["direction"])
            equity_curve[t] = float(position["capital_entry"] * (1.0 + direction * (day_close / entry_price_adj - 1.0)))
        else:
            equity_curve[t] = capital

        if position is None:
            sig = signals[t]
            action = sig.get("action")
            if action not in ("BUY", "SELL"):
                continue
            if action == "SELL" and not allow_short:
                continue

            # Need next bar for entry price.
            next_bar = df_ind.iloc[t + 1]
            entry_open = float(next_bar.get("open", np.nan))
            if pd.isna(entry_open) or entry_open <= 0:
                entry_open = float(next_bar["close"])

            sl = sig.get("stop_loss")
            tp = sig.get("take_profit")
            if sl is None or tp is None or pd.isna(sl) or pd.isna(tp):
                continue

            direction = 1 if action == "BUY" else -1

            # Slippage adjustment on entry price.
            if direction == 1:
                entry_price_adj = entry_open * (1.0 + slip_rate)
            else:
                entry_price_adj = entry_open * (1.0 - slip_rate)

            capital_entry = capital * (1.0 - fees_rate)

            position = {
                "direction": direction,
                "entry_index": t + 1,
                "signal_index": t,
                "entry_time": df_ind.index[t + 1].isoformat() if hasattr(df_ind.index[t + 1], "isoformat") else str(df_ind.index[t + 1]),
                "entry_price_adj": entry_price_adj,
                "stop_loss": float(sl),
                "take_profit": float(tp),
                "capital_entry": capital_entry,
                "fees_rate": fees_rate,
                "slip_rate": slip_rate,
                "max_exit_time_index": min(t + 1 + max_holding_days, len(df_ind) - 1),
                "score_entry": sig.get("score"),
                "confidence_entry": sig.get("confidence"),
                "action_entry": action,
            }
        else:
            # Manage open position using bar high/low at time t.
            direction = int(position["direction"])
            sl = float(position["stop_loss"])
            tp = float(position["take_profit"])

            # Time exit check uses the current bar index t.
            time_exit_due = t >= position["max_exit_time_index"]

            long_pos = direction == 1
            if long_pos:
                stop_hit = float(bar["low"]) <= sl
                tp_hit = float(bar["high"]) >= tp
            else:
                stop_hit = float(bar["high"]) >= sl
                tp_hit = float(bar["low"]) <= tp

            exit_reason = None
            exit_price = None
            exit_at_same_day = False

            if stop_hit and tp_hit:
                # Conservative: stop first.
                exit_reason = "stop"
                exit_price = sl
                exit_at_same_day = True
            elif stop_hit:
                exit_reason = "stop"
                exit_price = sl
                exit_at_same_day = True
            elif tp_hit:
                exit_reason = "take_profit"
                exit_price = tp
                exit_at_same_day = True
            else:
                # Opposite signal: decision at close(t), exit at next open(t+1).
                sig_now = signals[t]
                desired = sig_now.get("action")
                opposite = (desired == "SELL" and direction == 1) or (desired == "BUY" and direction == -1)
                if opposite:
                    exit_reason = "opposite_signal"
                    exit_at_same_day = False
                elif time_exit_due:
                    exit_reason = "time_exit"
                    exit_at_same_day = False

            if exit_reason is not None:
                if exit_at_same_day:
                    # Exit at stop/TP level (within the current bar).
                    raw_exit = float(exit_price)
                    if direction == 1:
                        exit_price_adj = raw_exit * (1.0 - slip_rate)
                    else:
                        exit_price_adj = raw_exit * (1.0 + slip_rate)
                    equity_exit = float(position["capital_entry"] * (1.0 + direction * (exit_price_adj / position["entry_price_adj"] - 1.0)))
                    equity_exit = equity_exit * (1.0 - fees_rate)
                    capital = equity_exit
                    equity_curve[t] = capital
                    exit_index = t
                else:
                    # Exit at next open (exit at t+1 open).
                    if t + 1 >= len(df_ind):
                        break
                    next_open = float(df_ind.iloc[t + 1].get("open", np.nan))
                    if pd.isna(next_open) or next_open <= 0:
                        next_open = float(df_ind.iloc[t + 1]["close"])
                    if direction == 1:
                        exit_price_adj = next_open * (1.0 - slip_rate)
                    else:
                        exit_price_adj = next_open * (1.0 + slip_rate)

                    equity_exit = float(position["capital_entry"] * (1.0 + direction * (exit_price_adj / position["entry_price_adj"] - 1.0)))
                    equity_exit = equity_exit * (1.0 - fees_rate)
                    capital_before = position["capital_entry"] / (1.0 - fees_rate) if (1.0 - fees_rate) > 0 else position["capital_entry"]
                    pnl = equity_exit - capital_before
                    capital = equity_exit

                    equity_curve[t + 1] = capital
                    exit_index = t + 1

                # Log trade.
                capital_before_trade = equity_curve[position["signal_index"]] if position["signal_index"] < len(equity_curve) else initial_capital
                pnl_abs = capital - (capital_before_trade if pd.notna(capital_before_trade) else initial_capital)
                ret_pct = (capital - (capital_before_trade if pd.notna(capital_before_trade) else initial_capital)) / (capital_before_trade if pd.notna(capital_before_trade) else initial_capital)

                trades.append(
                    {
                        "symbol": symbol,
                        "side": "LONG" if direction == 1 else "SHORT",
                        "entry_time": position["entry_time"],
                        "exit_time": df_ind.index[exit_index].isoformat() if hasattr(df_ind.index[exit_index], "isoformat") else str(df_ind.index[exit_index]),
                        "entry_price": round(float(position["entry_price_adj"]), 6),
                        "exit_price": round(float(exit_price_adj), 6),
                        "stop_loss": round(float(position["stop_loss"]), 6),
                        "take_profit": round(float(position["take_profit"]), 6),
                        "exit_reason": exit_reason,
                        "confidence_entry": position.get("confidence_entry"),
                        "score_entry": position.get("score_entry"),
                        "pnl_abs": round(float(pnl_abs), 6),
                        "return_pct": round(float(ret_pct), 6),
                    }
                )

                position = None

    # Final mark for last bar.
    if position is not None:
        last_close = float(df_ind["close"].iloc[-1])
        direction = int(position["direction"])
        equity_curve[-1] = float(position["capital_entry"] * (1.0 + direction * (last_close / float(position["entry_price_adj"]) - 1.0)))
        equity_curve[-1] = equity_curve[-1] * (1.0 - fees_rate)
        capital = float(equity_curve[-1])

    # Fill any remaining NaNs in equity curve with last known.
    for i in range(len(equity_curve)):
        if pd.isna(equity_curve[i]):
            equity_curve[i] = capital if i == len(equity_curve) - 1 else (equity_curve[i - 1] if i > 0 else initial_capital)

    daily_returns = np.diff(equity_curve) / equity_curve[:-1]
    total_return = float(equity_curve[-1] / initial_capital - 1.0)
    days = float((df_ind.index[-1] - df_ind.index[0]).days) if hasattr(df_ind.index[-1], "to_pydatetime") else float(len(df_ind))
    cagr = _cagr(initial_capital, float(equity_curve[-1]), days)
    sharpe = _sharpe(daily_returns)
    max_drawdown = float(_compute_drawdown(equity_curve))

    # Trade stats
    total_trades = len(trades)
    wins = [t for t in trades if t.get("return_pct", 0) > 0]
    losses = [t for t in trades if t.get("return_pct", 0) < 0]
    win_rate = float(len(wins) / total_trades) if total_trades else 0.0
    avg_win = float(np.mean([t["return_pct"] for t in wins])) if wins else None
    avg_loss = float(np.mean([t["return_pct"] for t in losses])) if losses else None
    gross_win = float(np.sum([t["return_pct"] for t in wins])) if wins else 0.0
    gross_loss = float(np.sum([abs(t["return_pct"]) for t in losses])) if losses else 0.0
    profit_factor = _safe_div(gross_win, gross_loss) if gross_loss else None
    expectancy = float(np.mean([t["return_pct"] for t in trades])) if total_trades else 0.0

    benchmark_return = float(benchmark_equity_end / initial_capital - 1.0)

    return {
        "symbol": symbol,
        "has_real_data": True,
        "params": {
            "start": start,
            "end": end,
            "period": period,
            "interval": interval,
            "initial_capital": initial_capital,
            "fees_bps": fees_bps,
            "slippage_bps": slippage_bps,
            "max_holding_days": max_holding_days,
            "allow_short": allow_short,
        },
        "metrics": {
            "total_trades": total_trades,
            "win_rate": win_rate,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "expectancy": expectancy,
            "profit_factor": profit_factor,
            "max_drawdown": max_drawdown,
            "total_return": total_return,
            "CAGR": cagr,
            "Sharpe": sharpe,
            "benchmark_buy_and_hold": benchmark_return,
        },
        "equity_curve": {
            "start_equity": float(equity_curve[valid_start_idx]),
            "end_equity": float(equity_curve[-1]),
        },
        "trade_log": trades,
    }


def run_backtest_batch(
    symbols: List[str],
    asset_metas: Dict[str, Dict[str, Any]],
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    period: str = "3y",
    interval: str = "1d",
    initial_capital: float = 10000.0,
    fees_bps: float = 5.0,
    slippage_bps: float = 2.0,
    max_holding_days: int = 10,
    allow_short: bool = True,
) -> Dict[str, Any]:
    per_symbol: Dict[str, Any] = {}
    all_trades: List[Dict[str, Any]] = []
    per_symbol_trades: Dict[str, List[Dict[str, Any]]] = {}

    for sym in symbols:
        meta = asset_metas.get(sym) or {"symbol": sym}
        res = run_backtest(
            sym,
            meta,
            start=start,
            end=end,
            period=period,
            interval=interval,
            initial_capital=initial_capital,
            fees_bps=fees_bps,
            slippage_bps=slippage_bps,
            max_holding_days=max_holding_days,
            allow_short=allow_short,
        )
        per_symbol[sym] = res
        trades = res.get("trade_log", []) or []
        per_symbol_trades[sym] = trades
        all_trades.extend(trades)

    def trade_stats(trades_in: List[Dict[str, Any]]) -> Dict[str, Any]:
        trades_in = [t for t in trades_in if t.get("return_pct") is not None]
        total_trades = len(trades_in)
        wins = [t for t in trades_in if t.get("return_pct", 0) > 0]
        losses = [t for t in trades_in if t.get("return_pct", 0) < 0]
        win_rate = float(len(wins) / total_trades) if total_trades else 0.0
        avg_win = float(np.mean([t["return_pct"] for t in wins])) if wins else None
        avg_loss = float(np.mean([t["return_pct"] for t in losses])) if losses else None
        expectancy = float(np.mean([t["return_pct"] for t in trades_in])) if total_trades else 0.0
        gross_win = float(np.sum([t["return_pct"] for t in wins])) if wins else 0.0
        gross_loss = float(np.sum([abs(t["return_pct"]) for t in losses])) if losses else 0.0
        profit_factor = _safe_div(gross_win, gross_loss) if gross_loss else None
        return {
            "total_trades": total_trades,
            "win_rate": win_rate,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "expectancy": expectancy,
            "profit_factor": profit_factor,
        }

    # Aggregate across all symbols.
    trades_all = [t for t in all_trades if t.get("return_pct") is not None]
    agg_trade_stats = trade_stats(trades_all)
    per_syms = [per_symbol[s] for s in symbols if s in per_symbol and per_symbol[s] and per_symbol[s].get("metrics")]
    total_return = float(np.mean([s["metrics"]["total_return"] for s in per_syms])) if per_syms else 0.0
    max_drawdown = float(np.min([s["metrics"]["max_drawdown"] for s in per_syms])) if per_syms else 0.0
    CAGR = float(np.mean([s["metrics"]["CAGR"] for s in per_syms if s["metrics"].get("CAGR") is not None])) if per_syms else None
    Sharpe_vals = [s["metrics"]["Sharpe"] for s in per_syms if s["metrics"].get("Sharpe") is not None]
    Sharpe = float(np.mean(Sharpe_vals)) if Sharpe_vals else None
    benchmark = float(np.mean([s["metrics"]["benchmark_buy_and_hold"] for s in per_syms])) if per_syms else 0.0

    # Aggregate by market/category (from asset metadata).
    by_market: Dict[str, Dict[str, Any]] = {}
    markets = {}
    for sym in symbols:
        markets[sym] = (asset_metas.get(sym) or {}).get("market", "Unknown")
    for m in set(markets.values()):
        group_syms = [s for s in symbols if markets.get(s) == m]
        group_trades = []
        group_metrics = []
        for gs in group_syms:
            group_trades.extend(per_symbol_trades.get(gs, []))
            if per_symbol.get(gs, {}).get("metrics"):
                group_metrics.append(per_symbol[gs]["metrics"])
        ms = trade_stats(group_trades)
        by_market[m] = {
            **ms,
            "max_drawdown": float(np.min([x["max_drawdown"] for x in group_metrics])) if group_metrics else 0.0,
            "total_return": float(np.mean([x["total_return"] for x in group_metrics])) if group_metrics else 0.0,
            "CAGR": float(np.mean([x["CAGR"] for x in group_metrics if x.get("CAGR") is not None])) if group_metrics else None,
            "Sharpe": float(np.mean([x["Sharpe"] for x in group_metrics if x.get("Sharpe") is not None])) if group_metrics else None,
            "benchmark_buy_and_hold": float(np.mean([x["benchmark_buy_and_hold"] for x in group_metrics])) if group_metrics else 0.0,
        }

    return {
        "params": {
            "symbols": symbols,
            "start": start,
            "end": end,
            "period": period,
            "interval": interval,
            "initial_capital": initial_capital,
            "fees_bps": fees_bps,
            "slippage_bps": slippage_bps,
            "max_holding_days": max_holding_days,
            "allow_short": allow_short,
        },
        "aggregate": {
            **agg_trade_stats,
            "max_drawdown": max_drawdown,
            "total_return": total_return,
            "CAGR": CAGR,
            "Sharpe": Sharpe,
            "benchmark_buy_and_hold": benchmark,
        },
        "aggregate_by_market": by_market,
        "per_symbol": per_symbol,
    }

