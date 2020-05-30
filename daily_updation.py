import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import numpy as np
import datetime


country_mapping = {
    'us':'United States',
    'brazil':'Brazil',
    'russia':'Russia',
    'spain':'Spain',
    'italy':'Italy',
    'france':'France',
    'germany':'Germany',
    'turkey':'Turkey',
    'india':'India',
    'iran':'Iran',
    'peru':'Peru',
    'canada':'Canada',
    'chile':'Chile',
    'china':'China',
    'mexico':'Mexico',
    'saudi-arabia':'Saudi Arabia',
    'pakistan':'Pakistan',
    'belgium':'Belgium',
    'qatar':'Qatar',
    'bangladesh':'Bangladesh',
    'belarus':'Belarus',
    'ecuador':'Ecuador',
    'sweden':'Sweden'
}


def date_check(page_content):
    """
    Checks the date for the most recent update for statistics on the webpage.
    
    parameters:
        page_content: str.
        HTML parsed contents of the page.
        
    returns:
        result: bool.
        Boolean.
    """
    
    page_date = page_content.find('div', class_= 'news_date').h4.contents[0]
    page_date = re.sub('\(.*\)','',page_date)
    date = datetime.datetime.strptime(page_date + "2020", '%b %d %Y').date()
    
    if date == datetime.datetime.today().date():
        
        return False
    
    return True


def updated_stats(url):
    """
    Gathers updated data on the number of cases and deaths in a day.
    
    paramters:
        url: str.
        URL from where the updates are scraped.
        
    returns:
        result: tuple.
        A tuple with total cases and total deaths of the day.
    """
    
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    if date_check(soup):
        
        return ()
    
    updated_list = soup.find('li', class_= 'news_li')
    updates = updated_list.find_all('strong')
    
    if 'new' in updates[0].contents[0]:
        
        daily_cases = updates[0].contents[0]
        daily_cases = re.sub('[, new cases]','',daily_cases)
        daily_cases = int(daily_cases)
        
    else:
        
        daily_cases = np.NaN
        
    if 'new' in updates[1].contents[0]:
        
        daily_deaths = updates[1].contents[0]
        daily_deaths = re.sub('[, new deaths]','',daily_deaths)
        daily_deaths = int(daily_deaths)
    
    else:
        
        daily_deaths = np.NaN
    
    
    result = (daily_cases, daily_deaths)
    
    return result



def daily_updates():
    """
    Scrape daily updates on covid-19 statistics and update data files.
    
    returns: bool.
    Boolean.
    """
    
    for country in country_mapping.keys():
        
        url = "https://www.worldometers.info/coronavirus/country/"+country+"/"
        updates = updated_stats(url)
        
        country_df = pd.read_csv('./Data/covid19_'+country+'_stats.csv')
        overall_data = pd.read_csv('./Data/covid19_overall_stat.csv')
        
        overall_county_data = overall_data[overall_data.country == country_mapping[country]]
        
        last_update = country_df.iloc[-1]
        new_update = last_update
        
        new_update.date = datetime.datetime.today().date()
        
        if updates:
            
            new_update.total_cases = last_update.total_cases + updates[0]
            new_update.daily_cases = updates[0]
            
            if not np.isnan(updates[1]):
                
                new_update.total_deaths = last_update.total_deaths + updates[1]
                new_update.daily_deaths = updates[1]
                
            else:
                
                new_update.daily_deaths = np.NaN
                
            new_update.active_cases = new_update.total_cases - (
                overall_county_data.total_recoveries.iloc[0] + overall_county_data.total_deaths.iloc[0]
            )
        
            country_df = country_df.append(new_update).reset_index(drop= True)
            country_df.to_csv('./Data/covid19_'+country+'_stats.csv', index=False)
            print('Successfully updated: ',country)
            
        else:
            
            print("No updates: ", country)
        
    return True


if __name__ == '__main__':
	
	daily_updates()
