import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import numpy as np
import datetime


countries = [
    'us','brazil','russia','spain',
    'italy','france','germany',
    'turkey','india','iran','peru',
    'canada','chile','china','mexico',
    'saudi-arabia','pakistan','belgium',
    'qatar', 'bangladesh',
    'belarus', 'ecuador', 'sweden'
]


data_indexes = {
                'total_cases':0,
                'daily_cases':1,
                'active_cases':2,
                'total_deaths':3,
                'daily_deaths':4
               }



def clean(string):
    """
    Clean the script tag contents for easier retrieval of data.
    
    paramters:
        string: str.
        The contents of the script tag.
        
    returns:
        string: str.
        The cleaned contents of the script tag.
    """
    
    string = re.sub("[\n \\\']",'',str(string))
    string = string.replace(" ",'')
    string = re.sub('[{}\[\]():]',' ',string)
    string = re.sub('[\"\" /*]','',string)
    
    return string


def retrieve_dates(string):
    """
    Retrieve dates from the cleaned script tag contents.
    
    parameters:
        string: str.
        The cleaned contents of the script tag.
        
    returns:
        dates: list.
        A list of the dates.
    """
    
    start_string = 'categories'
    end_string = ',yAxis'
    
    start_index = string.find(start_string) + len(start_string)
    end_index = string.find(end_string)
    
    dates = string[start_index:end_index].strip().split(",")
    
    return dates


def retrieve_daily_stats(string):
    """
    Retrieves daily statistics from the cleaned script tag contents.
    
    parameters:
        string: str.
        Cleaned contents of the script tag.
                
    returns:
        values: list.
        A list of daily statistics ordered by date.
    """
    
    start_string = 'data'
    end_string = ',name'
    
    start_index = string.find(start_string) + len(start_string)
    end_index = string.find(end_string)
    
    values = string[start_index:end_index].strip().split(",")
    
    return values


def retrieve_overall_stats(string):
    """
    Retrieves overall statistics from the cleaned script tag contents.
    
    parameters:
        string: str.
        Cleaned contents of the script tag.
        
    returns:
        values: list.
        A list of daily statistics ordered by date.
    """
    
    start_string = 'data'
    end_string = ',resp'
    
    start_index = string.find(start_string) + len(start_string)
    end_index = string.find(end_string)
    
    values = string[start_index:end_index].strip().split(",")
    
    return values


def page_contents(url):
    """
    Retrieves contents of the web page from the specified url and the specific div tag class - col-md-12
    
    paramters:
        url: str.
        The url to the web page to be scrapped.
        
    returns:
        result: str.
        HTML parsed web page content as string.
    """
    
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    result = soup.find_all('div', class_= 'col-md-12')
    
    return result


def script_tag_contents(page_content, stat):
    """
    Retrieves the script tag contents from the web page contents.
    
    paramters:
        page_content: str.
        HTML parsed web page contents
        
        stat: str.
        String specifying the kind of statistic from the data_indexes.
        
    returns:
        script_content: str.
        Script tag contents as string.
    """
    
    stat_data = page_content[data_indexes[stat]]
    script_content = stat_data.find('script').contents[0]
    
    return script_content



def build_dataframe(values,stat_name,dataframe=None,date=None):
    """
    Build a DataFrame from the dates and the values scraped.
    
    parameters:
        dataframe: DataFrame.
        A DataFrame containing dates and/or statistics.
        
        date: list.
        List of dates for the statistics in string format.
        
        values: list:
        List of values (data) for the statistics in string format.
        
        stat_name: str.
        Name of the statistic, for which the list of values are passed.
        
        returns:
            dataframe: DataFrame.
            DataFrame containing dates and passed statistic values.
    """
    
    if dataframe is None and date is not None:
        
        dataframe = pd.DataFrame({'date':date, stat_name:values})
        
    else:
        
        dataframe[stat_name] = values
        
    return dataframe



def clean_date(dataframe, date_col):
    """
    Clean the date column in the dataframe to standard date representation - YYYY-MM-DD
    
    parameters:
        dataframe: DataFrame.
            DataFrame whose dates are to be cleaned.
            
        date_col: str.
        Name of the date column in the DataFrame.
        
    returns:
        dataframe: DataFrame.
        Cleaned DataFrame.
    """
    
    dataframe[date_col] = dataframe[date_col].apply(
        lambda date: date[:3]+" "+date[3:]+" 2020"
    )
    dataframe[date_col] = dataframe[date_col].apply(
        lambda date: datetime.datetime.strptime(date,'%b %d %Y').date()
    )
    
    return dataframe



def scrape_data():
    """
    Scrape the web page for date, total cases, daily cases, total active cases, total_deaths, daily deaths
    daily recoveries per country. Creates a folder in local directory containing csv files per country with 
    the respective data. Website - worldometers.info
    
    parameters: None
    
    returns: bool.
    boolean.
    """
    
    has_been_run_once = False
        
    for country in countries:
                
        url = "https://www.worldometers.info/coronavirus/country/"+country+"/"
        content = page_contents(url)
        
        for stat in data_indexes:
                        
            script_contents = script_tag_contents(content, stat)
            script_contents = clean(script_contents)
            
            if 'daily' in stat:
                
                data = retrieve_daily_stats(script_contents)
            
            else:
                
                data = retrieve_overall_stats(script_contents)
                
            if not has_been_run_once:
                
                date = retrieve_dates(script_contents)
                dataframe = build_dataframe(data, stat, date= date)
                has_been_run_once = True
                
            else:
                
                dataframe = build_dataframe(data, stat, dataframe= dataframe)
            
        dataframe = clean_date(dataframe, date_col='date')
        dataframe.to_csv('./Data/covid19_'+country+'_stats.csv',index=False)
        has_been_run_once = False
        print("Scraped successfully: ",country)
    
    return True


if __name__ == '__main__':

	scrape_data()