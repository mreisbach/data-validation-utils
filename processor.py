"""Validation processor module."""
import numpy as np


def run_validation(config, chunk_id):
    """Run validation batch."""
    np.random.seed(config.get("seed", 42) + chunk_id)

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

    chunk_size = len(series_data) // total_chunks
    start_idx = chunk_id * chunk_size
    end_idx = start_idx + chunk_size if chunk_id < total_chunks - 1 else len(series_data)
    chunk_series = series_data[start_idx:end_idx]

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
