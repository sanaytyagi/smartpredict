import pandas as pd
import math
import os
import asyncio
import sys
import datetime


def _safe_read_csv(path, **kwargs):
    try:
        return pd.read_csv(path, **kwargs)
    except FileNotFoundError:
        print(f"Warning: {path} not found. Continuing with empty DataFrame.")
        return pd.DataFrame()


def generate_recommendations():
    """Read the three data CSVs and produce buy/sell recommendation CSVs with confidence scores.

    Data sources (weights):
      - Congress purchases/sales: weight 1.0
      - Insider purchases/sales: weight 2.5
      - Hedge fund purchases/sales: weight 1.5

    Outputs:
      - data/buy_recommendations.csv
      - data/sell_recommendations.csv
    """
    os.makedirs('data', exist_ok=True)

    congress_df = _safe_read_csv("data/congress_trades.csv")
    hedge_fund_df = _safe_read_csv("data/hedge_fund_trades.csv")
    insider_df = _safe_read_csv("data/insider_trades.csv")

    # Helper to get buy/sell series
    def _filter_counts(df, column_name, buy_token="Purchase", sell_token="Sale"):
        # Return empty series if df missing or expected columns absent
        if df.empty or column_name not in df.columns or 'ticker' not in df.columns:
            return pd.Series(dtype=int), pd.Series(dtype=int)

        # Normalize column text and use regex whole-word matching so variants
        # like "Sale (Full)" or "Purchase (Partial)" are matched.
        col_series = df[column_name].astype(str).str.strip()
        buy_pattern = r"\b(?:purchase|buy)\b"
        sell_pattern = r"\b(?:sale|sell)\b"

        buys = df[col_series.str.contains(buy_pattern, case=False, na=False, regex=True)]
        sells = df[col_series.str.contains(sell_pattern, case=False, na=False, regex=True)]

        buy_counts = buys['ticker'].astype(str).str.upper().value_counts()
        sell_counts = sells['ticker'].astype(str).str.upper().value_counts()
        return buy_counts, sell_counts

    # Congress uses 'transaction_type'
    congress_buy_counts, congress_sell_counts = _filter_counts(congress_df, 'transaction_type')
    # Insider uses 'transaction_type' (if present)
    insider_buy_counts, insider_sell_counts = _filter_counts(insider_df, 'transaction_type')
    # Hedge funds use 'action' column with 'Purchase'/'Sale'
    hedge_buy_counts, hedge_sell_counts = _filter_counts(hedge_fund_df, 'action')

    # Compute weighted buy and sell tallies separately, then use net scoring
    buy_weighted = {}
    sell_weighted = {}

    def _accumulate_to(counts, dest, weight):
        for ticker, freq in counts.items():
            if ticker and ticker != "-":
                dest[ticker] = dest.get(ticker, 0) + freq * weight

    _accumulate_to(congress_buy_counts, buy_weighted, 1.0)
    _accumulate_to(insider_buy_counts, buy_weighted, 2.5)
    _accumulate_to(hedge_buy_counts, buy_weighted, 1.5)

    _accumulate_to(congress_sell_counts, sell_weighted, 1.0)
    _accumulate_to(insider_sell_counts, sell_weighted, 1.8)
    _accumulate_to(hedge_sell_counts, sell_weighted, 1.5)

    # Collect union of tickers
    tickers = set(list(buy_weighted.keys()) + list(sell_weighted.keys()))

    raw_buy_scores = {}   # positive = net buy signal (b - s)
    raw_sell_scores = {}  # positive = net sell signal (s - b)
    total_activity = {} 

    for t in tickers:
        b = float(buy_weighted.get(t, 0.0))
        s = float(sell_weighted.get(t, 0.0))
        total = b + s
        total_activity[t] = total

        # raw net scores
        net_buy = b - s   # > 0 => buy tilt
        net_sell = s - b  # > 0 => sell tilt

        if net_buy > 0:
            raw_buy_scores[t] = net_buy
        if net_sell > 0:
            raw_sell_scores[t] = net_sell
        
    MIN_TOTAL_WEIGHT = 1.5  # tune as needed (in terms of weighted events)

    # Max total activity across all tickers (for volume normalization)
    max_total_activity = max(total_activity.values()) if total_activity else 0.0

    # Compute combined strength scores that factor in tilt + volume
    buy_strength = {}   # 0–1 combined score for buy side
    sell_strength = {}  # 0–1 combined score for sell side

    if max_total_activity > 0:
        # BUY SIDE
        for t, raw_net in raw_buy_scores.items():
            total = total_activity.get(t, 0.0)
            if total < MIN_TOTAL_WEIGHT:
                continue  # too little data

            # Direction factor: how much buys dominate, 0–1
            direction_factor = raw_net / total  # since raw_net = b - s and raw_net > 0, 0–1

            # Volume factor: log-based normalization of total activity
            volume_factor = math.log1p(total) / math.log1p(max_total_activity)

            combined = direction_factor * volume_factor  # 0–1
            buy_strength[t] = combined

        # SELL SIDE
        for t, raw_net in raw_sell_scores.items():
            total = total_activity.get(t, 0.0)
            if total < MIN_TOTAL_WEIGHT:
                continue  # too little data

            # Direction factor: how much sells dominate, 0–1
            direction_factor = raw_net / total  # since raw_net = s - b and raw_net > 0, 0–1

            volume_factor = math.log1p(total) / math.log1p(max_total_activity)

            combined = direction_factor * volume_factor  # 0–1
            sell_strength[t] = combined

    # Convert combined strength into 0–100 confidence based on the strongest ticker on each side
    max_buy_strength = max(buy_strength.values()) if buy_strength else 0.0
    max_sell_strength = max(sell_strength.values()) if sell_strength else 0.0

    buy_rows = []
    for ticker, strength in sorted(buy_strength.items(), key=lambda x: x[1], reverse=True):
        raw_score = raw_buy_scores[ticker]
        confidence_pct = round(
            (strength / max_buy_strength) * 100.0 if max_buy_strength > 0 else 0.0,
            2
        )
        # Convert percentage to categorical label
        if confidence_pct >= 66:
            confidence = "High"
        elif confidence_pct >= 33:
            confidence = "Medium"
        else:
            confidence = "Low"
        buy_rows.append({
            'ticker': ticker,
            # Raw net score (buys - sells), keeps interpretability
            'score': raw_score,
            'confidence': confidence,
            'hedge_fund_buy_count': int(hedge_buy_counts.get(ticker, 0)),
            'insider_buy_count': int(insider_buy_counts.get(ticker, 0)),
            'congress_buy_count': int(congress_buy_counts.get(ticker, 0)),
            'hedge_fund_sell_count': int(hedge_sell_counts.get(ticker, 0)),
            'insider_sell_count': int(insider_sell_counts.get(ticker, 0)),
            'congress_sell_count': int(congress_sell_counts.get(ticker, 0))
        })

    sell_rows = []
    for ticker, strength in sorted(sell_strength.items(), key=lambda x: x[1], reverse=True):
        raw_score = raw_sell_scores[ticker]
        confidence_pct = round(
            (strength / max_sell_strength) * 100.0 if max_sell_strength > 0 else 0.0,
            2
        )
        # Convert percentage to categorical label
        if confidence_pct >= 66:
            confidence = "High"
        elif confidence_pct >= 33:
            confidence = "Medium"
        else:
            confidence = "Low"
        sell_rows.append({
            'ticker': ticker,
            # Raw net score (sells - buys)
            'score': raw_score,
            'confidence': confidence,
            'hedge_fund_sell_count': int(hedge_sell_counts.get(ticker, 0)),
            'insider_sell_count': int(insider_sell_counts.get(ticker, 0)),
            'congress_sell_count': int(congress_sell_counts.get(ticker, 0)),
            'hedge_fund_buy_count': int(hedge_buy_counts.get(ticker, 0)),
            'insider_buy_count': int(insider_buy_counts.get(ticker, 0)),
            'congress_buy_count': int(congress_buy_counts.get(ticker, 0))
        })

    buy_df = pd.DataFrame(buy_rows).sort_values(by='confidence', ascending=False)
    sell_df = pd.DataFrame(sell_rows).sort_values(by='confidence', ascending=False)

    buy_df.to_csv('data/buy_recommendations.csv', index=False)
    sell_df.to_csv('data/sell_recommendations.csv', index=False)

    print(f"Buy recommendations saved to data/buy_recommendations.csv ({len(buy_df)} rows)")
    print(f"Sell recommendations saved to data/sell_recommendations.csv ({len(sell_df)} rows)")
    return buy_df, sell_df


if __name__ == '__main__':
    print('Generating recommendations from CSV files...')
    generate_recommendations()


async def _run_script_async(script_path: str) -> None:
    """Run a Python script as an async subprocess and log output/errors."""
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if stdout:
            print(f"[{script_path}] stdout:\n{stdout.decode().strip()}")
        if stderr:
            print(f"[{script_path}] stderr:\n{stderr.decode().strip()}")
        if proc.returncode != 0:
            print(f"{script_path} exited with code {proc.returncode}")
    except Exception as e:
        print(f"Error running {script_path}: {e}")


async def auto_refresh_scraper(interval_minutes: int = (60 * 5)):
    scripts = [
        os.path.join('scripts', 'insider_data_scraper.py'),
        os.path.join('scripts', 'congress_data_scraper.py'),
        os.path.join('scripts', 'hedge_fund_data_scraper.py'),
    ]

    first = True
    while True:
        start_ts = datetime.datetime.utcnow().isoformat()
        print(f"Auto-refresh run starting: {start_ts} UTC")

        # Run scrapers sequentially
        for s in scripts:
            await _run_script_async(s)

        # Rebuild recommendations from the freshly written CSVs
        try:
            generate_recommendations()
            print("Re-generated recommendations successfully.")
        except Exception as e:
            print(f"Error generating recommendations: {e}")

        if interval_minutes <= 0:
            print("auto_refresh_scraper: interval <= 0, exiting after single run.")
            break

        # On first run, if caller wanted a small delay before repeating, allow regular sleep
        await asyncio.sleep(interval_minutes * 60)
