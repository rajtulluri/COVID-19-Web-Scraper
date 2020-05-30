import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import numpy as np
import datetime


def table_contents(url):
    """
    Retrieve contents of the table on the url.
    
    parameters:
        url: str.
        URL of the webpage. The table contents are scraped from this.
        
    returns:
        table_data: str.
        HTML parsed table data in string format.
    """
    
    page_content = requests.get(url)
    soup = BeautifulSoup(page_content.content, 'html.parser')
    table_data = soup.find('table', id='thetable')
    
    return table_data


def country_names(table_data):
    """
    Scrape country names from the table contents scraped from the Wikipedia url.
    
    parameters:
        table_data: str.
        Scraped table data from webpage in string format.
        
    returns:
        countries: list.
        A list of country names from the table on webpage.
    """
    
    countries= []
    table_head_data = table_data.find_all('th', scope= 'row')
    
    for data in table_head_data:
        
        anchor_data = data.find('a')
        
        if anchor_data is not None:
            
            countries.append(anchor_data.contents[0])
    
    return countries


def chunks(lst, size):
    """
    Segments the input list into equal chunks of specified size.
    
    parameters:
        lst: list.
        List to be segmented.
        
        size: int.
        Size of the chunks.
    """
    
    for i in range(0, len(lst), size):
        
        yield lst[i:i+size]
        


def clean_stats(stats):
    """
    Clean the statistics scraped from the webpage. Remove ',' and '\n'.
    
    parameters:
        stats: list.
        A list of statistics.
        
    returns:
        cleaned_stats: list.
        A cleaned list of statistics.
    """
    
    cleaned_stats = []
    
    for stat in stats:
        
        stat = re.sub(',','',stat)
        stat = stat.rstrip()
        cleaned_stats.append(stat)
        
    cleaned_stats.pop()
    
    return cleaned_stats


def table_statistics(table_data):
    """
    Scrape overall statistics on total cases, total deaths and total recoveries from 
    the table contents on the webpage, for each country.
    
    parameters:
        table_data: str.
        Scraped table data in string format.
        
    returns:
        stats: list.
        A list of lists giving country wise statistics.
    """
    
    stats = []
    row_data = table_data.find_all('td')
    
    for data in row_data:
        
        if data.find('sup'):
            
            continue
            
        if data.find('span'):
            
            stats.append('null')
            continue
        
        stats.append(data.contents[0])
        
    stats = clean_stats(stats)    
    stats = list(chunks(stats,3))
        
    return stats



def scrape_overall_data():
    """
    Scrape overall statistics country wise from the Wikipedia page on COVID-19 pandemic into a DataFrame.
    Writes the DataFrame to a csv file.
    
    returns: bool.
    
    """
    
    url = 'https://en.wikipedia.org/wiki/COVID-19_pandemic_by_country_and_territory'
    table_data = table_contents(url)
    
    countries = country_names(table_data)
    statistics = table_statistics(table_data)
    
    statistics_dict = {}
    
    for country, statistic in zip(countries, statistics):
        
        statistics_dict[country] = statistic
        
    dataframe = pd.DataFrame.from_dict(statistics_dict, orient= 'index', columns= [
        'total_cases',
        'total_deaths',
        'total_recoveries'
    ]).reset_index()
    
    dataframe.rename(columns= {'index':'country'}, inplace= True)
    dataframe.to_csv('./Data/covid19_overall_stat.csv', index= False)
    
    print("Successfully scraped table")
    
    return True




if __name__ == '__main__':

    scrape_overall_data()