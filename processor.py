"""Validation processor module."""
import math
import numpy as np


def _split_chunk(items, chunk_id, total_chunks):
    if total_chunks <= 1:
        return items
    size = len(items) // total_chunks
    start = chunk_id * size
    end = start + size if chunk_id < total_chunks - 1 else len(items)
    return items[start:end]


def _calculate_fitness(metrics, weights):
    score = 0.0
    for metric, weight in weights.items():
        value = metrics.get(metric, 0.0)
        if metric == "max_drawdown":
            value = 1.0 - min(1.0, abs(value))
        score += weight * value
    return score


def _run_log_metrics(config, chunk_id):
    entries = config.get("entries", [])
    total_chunks = config.get("chunks", 1)
    chunk_entries = _split_chunk(entries, chunk_id, total_chunks)
    weights_by_lineage = config.get("fitness_weights_by_lineage", {})

    fitness = {}
    processed = 0

    for entry in chunk_entries:
        bot_id = entry.get("bot_id")
        if not bot_id:
            continue

        pnl_points = entry.get("pnl_points", [])
        if not pnl_points:
            pnl_points = [0.0]

        returns = [b - a for a, b in zip(pnl_points, pnl_points[1:])]
        nonzero_returns = [r for r in returns if abs(r) > 1e-9]

        wins = sum(1 for r in nonzero_returns if r > 0)
        win_rate = (wins / len(nonzero_returns)) if nonzero_returns else 0.0

        sharpe = 0.0
        if len(nonzero_returns) >= 2:
            mean = sum(nonzero_returns) / len(nonzero_returns)
            variance = sum((r - mean) ** 2 for r in nonzero_returns) / (len(nonzero_returns) - 1)
            std = math.sqrt(max(variance, 0.0))
            if std > 1e-9:
                sharpe = (mean / std) * math.sqrt(len(nonzero_returns))

        peak = pnl_points[0]
        max_dd_abs = 0.0
        for p in pnl_points:
            peak = max(peak, p)
            max_dd_abs = max(max_dd_abs, peak - p)
        denom = max(1.0, abs(peak))
        max_drawdown = min(1.0, max_dd_abs / denom)

        pnl_final = float(pnl_points[-1])
        metrics = {
            "pnl_realized": 0.0,
            "pnl_unrealized": pnl_final,
            "trades_count": float(entry.get("trades_count", len(nonzero_returns))),
            "win_rate": win_rate,
            "sharpe": sharpe,
            "max_drawdown": max_drawdown,
            "error_count": float(entry.get("error_count", 0)),
            "total_return": pnl_final,
        }

        lineage = entry.get("lineage", "")
        weights = weights_by_lineage.get(lineage, {})
        metrics["fitness_score"] = _calculate_fitness(metrics, weights) if weights else 0.0

        fitness[bot_id] = metrics
        processed += 1

    return {
        "chunk": chunk_id,
        "job_type": "log_metrics",
        "fitness": fitness,
        "summary": {
            "processed": processed,
            "total_entries": len(chunk_entries),
        },
    }


def _run_backtest_validation(config, chunk_id):
    params = config.get("params", {})
    series_data = config.get("series", [])
    total_chunks = config.get("chunks", 1)

    w = params.get("w", 4)
    t = params.get("t", 0.01)
    tp = params.get("tp", 0.025)
    sl = params.get("sl", 0.015)
    mb = params.get("mb", 6)

    if not series_data:
        return {"chunk": chunk_id, "metrics": [], "error": "no data"}

    chunk_series = _split_chunk(series_data, chunk_id, total_chunks)

    results = []
    for entry in chunk_series:
        symbol = entry.get("id", "unknown")
        prices = entry.get("values", [])

        if len(prices) < w + 1:
            continue

        trades = []
        i = w
        while i < len(prices):
            window = prices[i - w : i]
            sma = sum(window) / w
            price = prices[i]
            dev = (price - sma) / sma if sma else 0

            if abs(dev) > t:
                direction = -1 if dev > 0 else 1
                entry_price = price
                exit_price = None
                exit_reason = None

                for j in range(1, mb + 1):
                    if i + j >= len(prices):
                        exit_price = prices[-1]
                        exit_reason = "eod"
                        break

                    future_price = prices[i + j]
                    pnl_pct = (future_price - entry_price) / entry_price * direction

                    if pnl_pct >= tp:
                        exit_price = future_price
                        exit_reason = "tp"
                        break
                    elif pnl_pct <= -sl:
                        exit_price = future_price
                        exit_reason = "sl"
                        break

                if exit_price is None:
                    exit_price = prices[min(i + mb, len(prices) - 1)]
                    exit_reason = "timeout"

                pnl = (exit_price - entry_price) / entry_price * direction
                trades.append({"pnl": pnl, "reason": exit_reason})
                i += mb
            else:
                i += 1

        if trades:
            wins = [t for t in trades if t["pnl"] > 0]
            total_pnl = sum(t["pnl"] for t in trades)
            results.append({
                "id": symbol,
                "n": len(trades),
                "w": len(wins),
                "pnl": round(total_pnl, 6),
            })

    summary = {
        "total_series": len(chunk_series),
        "total_trades": sum(r["n"] for r in results),
        "total_wins": sum(r["w"] for r in results),
        "total_pnl": sum(r["pnl"] for r in results),
    }

    return {"chunk": chunk_id, "metrics": results, "summary": summary}


def run_validation(config, chunk_id):
    """Run validation batch for a supported job type."""
    np.random.seed(config.get("seed", 42) + chunk_id)

    job_type = config.get("job_type", "backtest")
    if job_type == "log_metrics":
        return _run_log_metrics(config, chunk_id)

    return _run_backtest_validation(config, chunk_id)
