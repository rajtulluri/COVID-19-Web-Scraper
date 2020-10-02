# COVID-19-Web-Scraper
A Web scraper written in python that collects overall and country wise covid-19 statistics from https://www.worldometers.info/coronavirus/ and Wikipedia.
The scraper collects country wise statistics from 15th Feb to current date, such as :-

 * Total cases
 * Total deaths
 * Daily cases
 * Daily deaths
 * Active cases

And overall statistics for all countries such as :-

  * Total cases
  * Total recoveries
  * Total deaths
  
The repo also contains code to write these data files into a postgreSQL database running on localhost.
The files - country_stats.py and overall_stats.py make csv datasets scraping all data since Feb 15 to date. The file daily_updation.py daily updates these datasets made above with the current date statistics.
