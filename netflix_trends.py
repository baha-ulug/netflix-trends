import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
import os
import base64
from requests import post
import json
from datetime import datetime
import time
import psycopg2
import omdb

load_dotenv()

HOST = os.getenv("DB_HOST")
DATABASE = os.getenv("DB_DATABASE")
SCHEMA = os.getenv("DB_SCHEMA")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
PORT = os.getenv("DB_PORT")
TABLE = os.environ.get("DB_TABLE")
OMDB_API_KEY = os.environ.get("OMDB_API_KEY")
formatted_date = datetime.now().strftime("%Y.%m.%d %H:%M:%S")

def get_tsv_file():
    url = "https://top10.netflix.com/data/all-weeks-countries.tsv"
    url_global = "https://top10.netflix.com/data/all-weeks-global.tsv"
    response = requests.get(url)

    with open("all-weeks-countries.tsv", "wb") as f:
        f.write(response.content)
        # create an IMDb access object
    
    response = requests.get(url_global)
    with open("all-weeks-global.tsv", "wb") as f:
        f.write(response.content)
        # create an IMDb access object

def get_maxdate_db():
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)

    # Open a cursor to perform database operations
    cur = conn.cursor()

    try:
        # Execute the query to get the most recent date from a specific column in a table
        cur.execute(f"SELECT MAX(week) FROM {SCHEMA}.{TABLE}")

        # Fetch the results and store them in a variable
        most_recent_date = cur.fetchone()[0]
    except:
        most_recent_date = None

    # Close the cursor and the database connection
    cur.close()
    conn.close()

    # Print the most recent date
    print("The most recent date is:", most_recent_date)
    return most_recent_date

def read_tsv(most_recent_date):

    ##FOR COUNTRIES
    # Load the TSV file into a Pandas DataFrame
    df_countries = pd.read_csv('all-weeks-countries.tsv', sep='\t') 
    
    # Convert the date column to datetime format
    df_countries['week'] = pd.to_datetime(df_countries['week'])

    ##FOR GLOBAL
    # Load the TSV file into a Pandas DataFrame
    df_global = pd.read_csv('all-weeks-global.tsv', sep='\t') 
    df_global['week'] = pd.to_datetime(df_global['week'])
    df_global.insert(0, 'country_name', 'global')
    df_global.insert(1, 'country_iso2', 'GL')

    # concatenate the dataframes vertically
    df_united = pd.concat([df_countries, df_global], ignore_index=True)
    df_united['weekly_hours_viewed'] = df_united['weekly_hours_viewed'].fillna(0).replace([np.inf, -np.inf], 0)
    df_united['weekly_hours_viewed'] = df_united['weekly_hours_viewed'].astype(int)
    
    given_countries = ['TR', 'PK', 'AE', 'SA', 'GL']
    #second_date = "2023-03-05"
    #second_date = datetime.strptime(second_date, '%Y-%m-%d')
    
    # Filter the DataFrame by country and assign to a new variable
    #df_united = df_united.loc[(df_united['week'] > most_recent_date) & (df_united['week'] < second_date) & (df_united['country_iso2'].isin(given_countries))]
    df_united = df_united.loc[(df_united['week'] > most_recent_date)  & (df_united['country_iso2'].isin(given_countries))]

    df_united['scrape_date'] = formatted_date
    return df_united

def get_genres(df):
    # Set your API key for OMDb API
    omdb.set_default('apikey', OMDB_API_KEY)
    genres = []
    for title in df["show_title"]:
        try:
            # Search for the movie using its title
            search = omdb.search_movie(title)

            #print(search)
            #print(search[0]["title"])

            # Retrieve the first result (assumes it's the correct movie)
            movie = omdb.get(title=search[0]["title"])
            
            # Retrieve the genres of the movie
            genre = movie.get("genre", "")
            
            # Print the title and genres of the movie
            print(f"{title}: {genre}") 
        except:
            genre = None  
            print(f"{title}: {genre}")
        
        genres.append(genre)
    # add the genres as a new column to the dataframe
    df['genre'] = genres
    return df        

def db_insert(df):
    conn = None
    # Connect to the database
    print("before connection")
    conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
    print("after connection")
    print("connection is created")

    # Create a cursor object
    cur = conn.cursor()
    print("cursor is created")
    # Create schema if not exists
    cur.execute(f'''CREATE SCHEMA IF NOT EXISTS {SCHEMA}''')

    # Create table if not exists
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
        id serial4 NOT NULL, 
        country_name text, 
        country_iso2 text, 
        week timestamp, 
        category text, 
        weekly_rank integer, 
        show_title text, 
        season_title text, 
        cumulative_weeks_in_top_10 integer,
        weekly_hours_viewed integer,
        scrape_date timestamp, 
        genre text,
        CONSTRAINT {TABLE}_pkey PRIMARY KEY (id))""")

    query = f"""INSERT INTO {SCHEMA}.{TABLE} (country_name, country_iso2, week, category, weekly_rank, show_title, season_title, cumulative_weeks_in_top_10, weekly_hours_viewed, scrape_date, genre) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    # Use a loop to insert the data into the database
    for index, raw in df.iterrows():
        values = (raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10])
        print("values are: ",values)
        cur.execute(query, values)  
    print(f"Inserted row with ID")
    
    # Commit the changes to the database
    conn.commit()
    print("changes are commited")

    # Close the cursor and connection
    cur.close()
    conn.close()
    print("cursor and connection are closed")

def main():    
    get_tsv_file()
    most_recent_date = get_maxdate_db()
    #FOR FIRST RUN:
    if most_recent_date is None:
        most_recent_date = "2022-12-31"
        most_recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
        print(most_recent_date)

    df = read_tsv(most_recent_date)
    print("shape of df is:  ", df.shape)
    df = get_genres(df)
    db_insert(df)
    return "Success!"

if __name__=='__main__':
    main()