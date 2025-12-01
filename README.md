# ds3022-data-project-3
This repository contains three scripts:
1) finance-prodcuer.py: Connects to Alpaca's streaming API and publishes live trade data to Kafka
2) finance-consumer.py: Consumes messages from Kafka and writes them to DuckDB in batches
3) finance-analysis.py: Analyzes stored trades and generates visualizations

When run, these scripts connect to the Alpaca trades streaming API. They collect the streaming output of trades from tech companies, produce and consume them through Kafka, and write them out to DuckDB for analysis and plot creation. 

The analysis script generates three visualizations:
1) trade_activity_density.png - Density plot showing when trading activity occurs throughout the day
2) volatility_bar_chart.png - Volatility (standard deviation of returns) by security
3) return_vol_ratio_bar_chart.png - Return-to-volatility ratio comparison
- Additionally, it prints price statistics (high, low, median) and return metrics to console.
