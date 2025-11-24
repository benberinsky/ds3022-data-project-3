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
# 3. COMPUTE RETURNS FOR BETA CALCULATION
# ============================================================

# Percent returns by stock
df["return"] = df.groupby("trade_symbol")["price"].pct_change()

# Pivot to time-indexed returns matrix
#returns = df.pivot(index="time", columns="trade_symbol", values="return")
returns = df.groupby(['time', 'trade_symbol'])['return'].mean().unstack()


# Equal-weight market return
returns["market"] = returns.mean(axis=1)


# ============================================================
# 4. FUNCTION TO COMPUTE BETA
# ============================================================

def compute_beta(stock_ret, market_ret):
    """Compute beta = Cov(stock, market) / Var(market)."""
    joined = pd.concat([stock_ret, market_ret], axis=1).dropna()
    if len(joined) < 2:
        return np.nan
    cov = np.cov(joined.iloc[:, 0], joined.iloc[:, 1])[0, 1]
    var = np.var(joined.iloc[:, 1])
    return cov / var if var != 0 else np.nan


# ============================================================
# 5. COMPUTE 10-, 30-, 60-PERIOD BETAS
# ============================================================

betas = {10: {}, 30: {}, 60: {}}

symbols = [c for c in returns.columns if c != "market"]

for symbol in symbols:
    for window in [10, 30, 60]:
        # Get the last window of data for this symbol and market
        stock_ret = returns[symbol].dropna()
        market_ret = returns["market"].dropna()
        
        # Align the series by time index
        aligned = pd.concat([stock_ret, market_ret], axis=1).dropna()
        
        if len(aligned) < window:
            betas[window][symbol] = np.nan
            continue
            
        # Get the last window of data
        stock_window = aligned.iloc[-window:, 0]
        market_window = aligned.iloc[-window:, 1]
        
        # Compute beta on the actual returns, not means
        beta_val = compute_beta(stock_window, market_window)
        betas[window][symbol] = beta_val


# ============================================================
# 6. PRINT BETAS
# ============================================================

print("\n==================== BETAS ====================\n")

for window in [10, 30, 60]:
    print(f"--- {window}-Period Beta Estimates ---")
    for symbol, beta_val in betas[window].items():
        print(f"{symbol}: {beta_val:.4f}")
    print()

print("\n==================== DATA SUMMARY ====================")
for symbol in symbols:
    count = returns[symbol].dropna().shape[0]
    print(f"{symbol}: {count} return observations")
print(f"Market: {returns['market'].dropna().shape[0]} return observations")


# ============================================================
# DONE
# ============================================================

print("Analysis complete.")
