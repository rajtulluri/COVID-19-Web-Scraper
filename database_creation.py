import requests
from bs4 import BeautifulSoup
import json
import re
import pandas as pd
import numpy as np
import datetime
from psycopg2 import connect


countries = [
    'us','brazil','russia','spain',
    'italy','france','germany',
    'turkey','india','iran','peru',
    'canada','chile','china','mexico',
    'saudi-arabia','pakistan','belgium',
    'qatar', 'bangladesh',
    'belarus', 'ecuador', 'sweden'
]


def connect_database(dbname, user='hp', password='test1234', host='127.0.0.1', autocommit=False):
    """
    Connect to the PostgreSQL database based on the parameters.
    
    parameters:
        dbname: str.
        Name of the database.
        
        user: str.
        Name of the user. Default hp.
        
        password: str.
        Password for the user to connect.
        
        host: str.
        The IP address of the hosted database. Default 127.0.0.1 (localhost).
        
        autocommit: Boolean.
        Set autocommit to True or False. Default False
        
    returns:
        conn: connection object.
        A connection object to the database.
    """
    
    conn = connect("dbname="+dbname+" user="+user+" password="+password+" host="+host)
    conn.autocommit = True
    
    return conn



def create_country_relations(conn):
    """
    Create relations for every country in the database, for covid-19 statistics.
    
    parameters:
        conn: connection object.
        A connection object to the database.
        
    returns: bool.
    Boolean.
    """
    
    cursor = conn.cursor()
    
    for country in countries:
        
        data = pd.read_csv('./Data/covid19_'+country+'_stats.csv')
        country = re.sub('-','',country)
        
        query = "DROP TABLE IF EXISTS "+country+"_stats;"
        cursor.execute(query)
        
        query = """
        CREATE TABLE """+country+"""_stats (
            date DATE PRIMARY KEY, 
            total_cases INT,
            daily_cases INT,
            active_cases INT,
            total_deaths INT,
            daily_deaths INT
        );"""
        
        cursor.execute(query)
        
        for ind,row in data.iterrows():
            
            row = row.fillna(0)
            
            query = """
            INSERT INTO """+country+"""_stats
            VALUES (
            '"""+row.date+"""',
            """+str(row.total_cases)+""",
            """+str(row.daily_cases)+""",
            """+str(row.active_cases)+""",
            """+str(row.total_deaths)+""",
            """+str(row.daily_deaths)+"""
            );"""
            
            cursor.execute(query)
            
        print('Table successfully created: ',country+'_stats')
        
    return True



def create_overall_relation(conn):
    """
    Create the relation containing overall statistics for all the countries.
    
    parameters:
        conn: connection object.
        A connection object to the database.
        
    returns: bool.
    Boolean.
    """
    
    cursor = conn.cursor()
    
    data = pd.read_csv('./Data/covid19_overall_stat.csv')
    
    query = "DROP TABLE IF EXISTS overall_stats;"
    cursor.execute(query)
        
    query = """
        CREATE TABLE overall_stats (
            country VARCHAR(25), 
            total_cases BIGINT,
            total_deaths BIGINT,
            total_recoveries BIGINT
        );
        """
    cursor.execute(query)
    
    for ind,row in data.iterrows():
        
        row = row.fillna(0)
        row.country = row.country.replace(' ','')
        
        query = """
            INSERT INTO overall_stats VALUES (
            '"""+row.country+"""',
            """+str(row.total_cases)+""",
            """+str(row.total_deaths)+""",
            """+str(row.total_recoveries)+"""
            );"""
        
        cursor.execute(query)
        
    print("Table successfully created")
    
    return True



def update_database(conn):
    """
    Updates the database with the daily updates on statistics for countries.
    
    parameters:
        conn: connection object.
        A connection object to the database.
        
    returns: bool.
    Boolean.
    """
    
    cursor = conn.cursor()
    
    for country in countries:
        
        query = "SELECT date FROM "+country.replace('-','')+"_stats ORDER BY date DESC LIMIT 1;"
        cursor.execute(query)
        last_date = cursor.fetchone()[0]
        
        if (pd.to_datetime(last_date).date() != datetime.datetime.today().date()):
            
            data = pd.read_csv('./Data/covid19_'+country+'_stats.csv')
            update = data.iloc[-1].fillna(0)
            
            if pd.to_datetime(update.date).date() != datetime.datetime.today().date():
                
                continue
            
            query = """
            INSERT INTO """+country.replace('-','')+"""_stats
            VALUES (
            '"""+update.date+"""',
            """+str(update.total_cases)+""",
            """+str(update.daily_cases)+""",
            """+str(update.active_cases)+""",
            """+str(update.total_deaths)+""",
            """+str(update.daily_deaths)+"""
            );
            """
            
            cursor.execute(query)
            
            print("Table",country+"_stats updated")
            
    
    return True
    


if __name__ == '__main__':

	conn = connect_database('covid19_stats', autocommit= True)
	create_country_relations(conn)
	create_overall_relation(conn)
	update_database(conn)