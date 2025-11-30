# Summary Writeup

### Names: 
Benjamin Berinsky, Will Wert

### Data Source: 
For our project we used the Alpaca trading API. The API provided us with live streaming information about trades, including the company abbreviation of the trade, the price(in USD), the size(how many shares were traded), and the timestamp at which the trade was made. 

### Challenges
- Some limitations come with using the Alpaca trades streaming API, mainly that we are only subscribed to IEX exchange. This is only representative of about 2-3% of the total market, meaning we are missing a lot of the trading population in our analysis. the main limitation here is our analysis is not fully representative of the volume of trades, but it should still provide a good sample once taking this into consideration.
- A challenge with running our scripts was they had to be active when the market was open, from 9:30 AM-4:00 PM EST. The scripts were run for two complete days to capture all trades on those days in that timeframe. We worked through this issue by being vigilant about starting the scripts before the markets open and keeping them active until they close.
- For our tools, we used Kafka to produce and consume the data from the live streaming API by filtering out which companies we would keep, and then writing out the trades to a table in DuckDB to perform our analysis. 

### Analysis


### Github repo link
[Repo Link](https://github.com/benberinsky/ds3022-data-project-3/tree/main)

### Plots
![Return Volatility Ratio](plots/return_vol_ratio_bar_chart.png)
![Volatility Chart](plots/volatility_bar_chart.png)
![Trade Activity by Time](plots/trade_activity_density.png)



