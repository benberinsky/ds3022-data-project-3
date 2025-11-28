# Summary Writeup

Necessary components:
- One page, written in markdown
- The names of your team members
- What data source did you work with?
- What challenges did this data choice present in data gathering, processing and analysis, and how did you work through them? What methods and tools did you use to work with this data?
- Offer a brief analysis of the data with your findings. Keep it brief, clear, and meaningful. 
- Include at least one compelling plot or visualization of your work.
- Link to your project GitHub repository.
- To submit your team's summary, fork this repository, then copy the "sample" directory in your fork give it a unique name. Next, edit the README.md file within that directory and include/embed your plots in that directory and in the README file as needed. Finally, submit a pull request for your changes to be incorporated back into the upstream source. Only one pull request per team is necessary.

### Names: 
Benjamin Berinsky, Will Wert

### Data Source: 
For our project we used the Alpaca trading API. The API provided us with live streaming information about trades, including the company abbreviation of the trade, the price(in USD), the size(how many shares were traded), and the timestamp at which the trade was made. 

### Challenges
- One challenge we faced in this project was ...
- For our tools, we used Kafka to produce and consume the data from the live streaming API by filtering out which companies we would keep, and then writing out the trades to a table in DuckDB to perform our analysis. 

### Analysis


### Github repo link


### Plot

