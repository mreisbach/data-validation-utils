"""Batch processing utilities for data validation."""
import math
import random

# Simulated historical data (SPY-like daily returns)
# In production, this would be fetched or bundled
PRICE_DATA = [1.0]
random.seed(42)
for _ in range(252 * 2):  # 2 years of daily data
    ret = random.gauss(0.0004, 0.012)  # ~10% annual, 19% vol
    PRICE_DATA.append(PRICE_DATA[-1] * (1 + ret))


def traits_to_config(traits: dict, lineage: str) -> dict:
    """Convert genetic traits to strategy config."""
    return {
        "position_size": 0.05 + (traits.get("risk_appetite", 0.5) * 0.15),
        "stop_loss": 0.02 + ((1 - traits.get("loss_tolerance", 0.5)) * 0.08),
        "take_profit": 0.10 + (traits.get("risk_appetite", 0.5) * 0.20),
        "hold_bars": int(1 + (traits.get("time_horizon", 0.5) * 20)),
        "entry_threshold": 0.01 + (traits.get("momentum_sensitivity", 0.5) * 0.02),
        "contrarian": traits.get("contrarian_bias", 0.0) > 0.5,
    }


def backtest(config: dict, prices: list) -> dict:
    """Run simple momentum backtest, return metrics."""
    equity = 1.0
    peak = 1.0
    trades = []
    position = None
    bars_held = 0

    for i in range(20, len(prices)):
        ret_20 = (prices[i] / prices[i - 20]) - 1
        signal = ret_20 > config["entry_threshold"]
        if config["contrarian"]:
            signal = not signal

        # Entry
        if position is None and signal:
            position = {"entry": prices[i], "entry_idx": i}
            bars_held = 0

        # Exit check
        elif position is not None:
            bars_held += 1
            pnl_pct = (prices[i] / position["entry"]) - 1

            exit_reason = None
            if pnl_pct >= config["take_profit"]:
                exit_reason = "tp"
            elif pnl_pct <= -config["stop_loss"]:
                exit_reason = "sl"
            elif bars_held >= config["hold_bars"]:
                exit_reason = "time"

            if exit_reason:
                equity *= (1 + pnl_pct * config["position_size"])
                trades.append({"pnl": pnl_pct, "reason": exit_reason})
                position = None

        peak = max(peak, equity)

    # Metrics
    if not trades:
        return {
            "sharpe": 0.0,
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "trade_count": 0,
        }

    returns = [t["pnl"] for t in trades]
    avg_ret = sum(returns) / len(returns)
    std_ret = math.sqrt(sum((r - avg_ret) ** 2 for r in returns) / len(returns)) if len(returns) > 1 else 0.01
    sharpe = (avg_ret / std_ret) * math.sqrt(252) if std_ret > 0 else 0

    return {
        "sharpe": round(sharpe, 4),
        "total_return": round(equity - 1, 4),
        "max_drawdown": round(1 - (equity / peak), 4),
        "win_rate": round(sum(1 for t in trades if t["pnl"] > 0) / len(trades), 4),
        "trade_count": len(trades),
    }


def run_validation(cfg: dict, chunk_id: int) -> dict:
    """Run backtest for a batch of bots."""
    bots = cfg.get("bots", [])
    weights = cfg.get("fitness_weights", {})
    chunks = cfg.get("chunks", 10)

    if not bots:
        return {"fitness": {}}

    # Split bots by chunk
    chunk_size = max(1, len(bots) // chunks)
    start = chunk_id * chunk_size
    end = start + chunk_size if chunk_id < chunks - 1 else len(bots)
    my_bots = bots[start:end]

    results = {"fitness": {}}

    for bot in my_bots:
        config = traits_to_config(bot.get("traits", {}), bot.get("lineage", "apex"))
        metrics = backtest(config, PRICE_DATA)

        # Weighted fitness
        fitness = 0.0
        for k, w in weights.items():
            v = metrics.get(k, 0)
            if k == "max_drawdown":
                v = -v  # Lower drawdown is better
            fitness += w * v

        results["fitness"][bot["bot_id"]] = {
            "fitness_score": round(fitness, 4),
            **metrics,
        }

    return results
