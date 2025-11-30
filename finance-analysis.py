import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ============================================================
# 1. LOAD DATA
# ============================================================

con = duckdb.connect("trades_info.duckdb")
df = con.execute("SELECT time, trade_symbol, price, size FROM trades").df()


# Check if we have data
if df.empty:
    print("ERROR: No data found in trades table")
    exit(1)

# Convert timestamp
df["time"] = pd.to_datetime(df["time"])
df = df.sort_values(["trade_symbol", "time"])

# Extract time-of-day in seconds for density plot
df["seconds"] = df["time"].dt.hour * 3600 + df["time"].dt.minute * 60 + df["time"].dt.second

print(f"Loaded {len(df)} trade records")

# ============================================================
# 2. DENSITY PLOT OF VOLUME BY TIME OF DAY
# ============================================================

plt.figure(figsize=(12, 7))

for symbol, group in df.groupby("trade_symbol"):
    if len(group) > 1:
        group["seconds"].plot.kde(label=symbol)

plt.title("Density of Trading Activity by Time of Day")
plt.xlabel("Time of Day (Seconds Since Midnight)")
plt.ylabel("Density")
plt.legend()
plt.tight_layout()
plt.savefig("trade_activity_density.png")
plt.close()

print("Saved plot: trade_activity_density.png")


# ============================================================
# 3. PRICE STATISTICS (High, Low, Median)
# ============================================================

print("\n===== PRICE STATISTICS =====")

price_stats = (
    df.groupby("trade_symbol")["price"]
    .agg(["max", "min", "median"])
    .rename(columns={"max": "High", "min": "Low", "median": "Median"})
)

for symbol, row in price_stats.iterrows():
    print(f"\n{symbol}:")
    print(f"  Daily High Price:  {row['High']:.4f}")
    print(f"  Daily Low Price:   {row['Low']:.4f}")
    print(f"  Median Price:      {row['Median']:.4f}")

# ============================================================
# 4. RETURN + VOLATILITY CALCULATIONS
# ============================================================

returns_data = []

for symbol, group in df.groupby("trade_symbol"):
    group = group.sort_values("time")

    # compute returns using consecutive trades
    group["return"] = group["price"].pct_change()

    mean_return = group["return"].mean()
    vol = group["return"].std()

    if vol is None or np.isnan(vol):
        ratio = np.nan
    else:
        ratio = mean_return / vol

    returns_data.append([symbol, mean_return, vol, ratio])

returns_df = pd.DataFrame(
    returns_data, 
    columns=["Symbol", "MeanReturn", "Volatility", "ReturnVolRatio"]
)

print("\n===== RETURN & VOLATILITY SUMMARY =====")
print(returns_df)

# ============================================================
# 5. BAR CHART: VOLATILITY
# ============================================================

plt.figure(figsize=(10, 6))
plt.bar(returns_df["Symbol"], returns_df["Volatility"])
plt.title("Volatility of Returns by Security")
plt.xlabel("Ticker")
plt.ylabel("Volatility (Std Dev of Returns)")
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("volatility_bar_chart.png")
plt.close()

print("Saved plot: volatility_bar_chart.png")

# ============================================================
# 6. BAR CHART: RETURN / VOLATILITY
# ============================================================

plt.figure(figsize=(10, 6))
plt.bar(returns_df["Symbol"], returns_df["ReturnVolRatio"])
plt.title("Return / Volatility Ratio by Security")
plt.xlabel("Ticker")
plt.ylabel("Return-to-Volatility Ratio")
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("return_vol_ratio_bar_chart.png")
plt.close()

print("Saved plot: return_vol_ratio_bar_chart.png")

# ============================================================
# DONE
# ============================================================

print("Analysis complete.")
