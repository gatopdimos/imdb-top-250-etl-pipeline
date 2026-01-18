# webscraper-postgres.py
# Imports
import pandas as pd
import numpy as np
import time
import os
from itertools import chain
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine, text
import math

# --- Selenium setup ---
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
chrome_options = Options()
chrome_options.add_argument("--no-sandbox") #google security mechanism
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-dev-shm-usage") #instead of shared memory use disk
chrome_options.add_argument(f"user-agent={user_agent}")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"]) #do not show the automation banner
chrome_options.add_experimental_option('useAutomationExtension', False) #not to load the Selenium's automation extension
driver = webdriver.Chrome(options=chrome_options) #use Chrome as webdriver

# --- Scrape Top 250 page ---
main_url = "https://www.imdb.com/search/title/?groups=top_250&count=250&sort=user_rating,desc"
driver.get(main_url)
WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ipc-metadata-list-summary-item")))
elements = BeautifulSoup(driver.page_source, "html.parser").find_all("li", {"class":"ipc-metadata-list-summary-item"})

# --- Scrape helper functions ---
def get_element_data(element):
    meta = element.find_all("span",{"class":"sc-b4f120f6-7 hoOxkw dli-title-metadata-item"})
    try: rank = element.find("h3",{"class":"ipc-title__text"}).text.split(".")[0]
    except: rank = np.nan
    try: title = element.find("h3",{"class":"ipc-title__text"}).text.split(".")[1]
    except: title = np.nan
    try: year = meta[0].text
    except: year = np.nan
    try: duration = meta[1].text
    except: duration = np.nan
    try: rating = meta[2].text
    except: rating = np.nan
    try: metascore = element.find("span",{"class":"sc-9fe7b0ef-0 hDuMnh metacritic-score-box"}).text
    except: metascore = np.nan
    try: imdb_score = element.find("div",{"class":"sc-17ce9e4b-0 ddMjUi sc-b4f120f6-2 iBNUYJ dli-ratings-container"}).text.split("\xa0")[0]
    except: imdb_score = np.nan
    try: views = element.find("div",{"class":"sc-17ce9e4b-0 ddMjUi sc-b4f120f6-2 iBNUYJ dli-ratings-container"}).text.split("\xa0")[1].split(")")[0].replace("(","")
    except: views = np.nan
    try: description = element.find("div",{"class":"ipc-html-content ipc-html-content--base sc-9d52d06f-0 bVMrTF title-description-plot-container"}).text
    except: description = np.nan
    try: details_url = "https://www.imdb.com"+element.find("div",{"class":"ipc-title ipc-title--base ipc-title--title ipc-title-link-no-icon ipc-title--on-textPrimary sc-87337ed2-2 dRlLYG dli-title with-margin"}).find("a")["href"]
    except: details_url = np.nan

    return {
        "rank": rank,
        "title": title,
        "description": description,
        "year": year,
        "duration": duration,
        "rating": rating,
        "metascore": metascore,
        "imdb_score": imdb_score,
        "views": views,
        "details_url": details_url
    }

# --- Extract Top 250 into DataFrame ---
movies = [get_element_data(el) for el in elements]
df_movies = pd.DataFrame(movies)

# --- Scrape movie details ---
def data_details(link):
    driver.get(link)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "h1"))) #search by tag h1 which in the respective page h1 is connencted with the movie title
    soup = BeautifulSoup(driver.page_source, "html.parser")
    data = soup.find_all("div",{"class":"sc-14a487d5-11 gFSFjL"})
    try: description = data[0].find("p",{"class":"sc-bf30a0e-3 uWiw sc-bf30a0e-4 dKgygM"}).find("span",{"class":"sc-bf30a0e-0 iOCbqI"}).text
    except: description = np.nan
    try: director = data[0].find("section",{"class":"sc-af040695-4 hSUcrs"}).find("div",{"class":"sc-af040695-3 dUiZpV"}).find("ul",{"class":"ipc-metadata-list ipc-metadata-list--dividers-all title-pc-list ipc-metadata-list--baseAlt"}).find_all("li")[0].find("a").text
    except: director = np.nan

    list_writers, list_stars = [], []
    try: writers = data[0].find_all("ul")[1].find_all("li")
    except: writers = []
    for w in writers: list_writers.append(w.text)
    try: stars = data[0].find_all("li", {"class":"ipc-inline-list__item"})
    except: stars = []
    for s in stars: list_stars.append(s.text)

    return {"description": description, "director": director, "writers": list_writers, "stars": list_stars}

movie_details = [data_details(link) for link in df_movies["details_url"]]
df_movie_details = pd.DataFrame(movie_details)

# --- Merge ---
df_merged = df_movies.merge(df_movie_details, on="description", how="left")

# --- Connect to Postgres ---
engine = create_engine(
    'postgresql+psycopg2://postgres:docker@postgresdb:5432/postgres'
)

# --- Create normalized tables ---
with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS movies (
        movie_id SERIAL PRIMARY KEY,
        rank INT,
        title VARCHAR(255),
        description TEXT,
        year INT,
        duration VARCHAR(50),
        rating VARCHAR(50),
        metascore INT,
        imdb_score DECIMAL(3,1),
        views VARCHAR(50),
        details_url VARCHAR(255) UNIQUE,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS people (
        person_id SERIAL PRIMARY KEY,
        name VARCHAR(255)
    );
    CREATE TABLE IF NOT EXISTS movie_director (
        movie_id INT REFERENCES movies(movie_id),
        person_id INT REFERENCES people(person_id),
        PRIMARY KEY (movie_id, person_id)
    );
    CREATE TABLE IF NOT EXISTS movie_writer (
        movie_id INT REFERENCES movies(movie_id),
        person_id INT REFERENCES people(person_id),
        PRIMARY KEY (movie_id, person_id)
    );
    CREATE TABLE IF NOT EXISTS movie_star (
        movie_id INT REFERENCES movies(movie_id),
        person_id INT REFERENCES people(person_id),
        PRIMARY KEY (movie_id, person_id)
    );
    """))

# --- Prepare people DataFrame ---
all_people = set(
    chain(
        chain.from_iterable(df_merged['writers'].dropna()),
        chain.from_iterable(df_merged['stars'].dropna()),
        df_merged['director'].dropna()
    )
)
df_people = pd.DataFrame({'name': list(all_people)})

# --- Insert people ---
with engine.begin() as conn:
    for _, row in df_people.iterrows():  # _ is the index of tuple created from the iterrows but we only care about the values 
        conn.execute(text("""
            INSERT INTO people (name) VALUES (:name) ON CONFLICT (name) DO NOTHING
        """), row.to_dict())

df_people_db = pd.read_sql("SELECT * FROM people", engine)

# helper function to translate person's name with their ID in the DB from the people table
def get_person_id(name):
    row = df_people_db.loc[df_people_db['name']==name,'person_id']
    if row.empty:
        return None
    return int(row.values[0])

# Utility functions to sanitize numeric values

def safe_int(val):
    try:
        if val is None or (isinstance(val,float) and math.isnan(val)):
            return None
        return int(val)
    except:
        return None

def safe_decimal(val):
    try:
        if val is None or (isinstance(val,float) and math.isnan(val)):
            return None
        return float(val)
    except:
        return None
    
# --- Insert movies ---
df_movies_norm = df_merged.drop(columns=['director','writers','stars'])
with engine.begin() as conn:
    for _, row in df_movies_norm.iterrows():
        rank = safe_int(row['rank'])
        year = safe_int(row['year'])
        metascore = safe_int(row['metascore'])
        imdb_score = safe_decimal(row['imdb_score'])
        # insert or update
        conn.execute(text("""
            INSERT INTO movies (rank, title, description, year, duration, rating, metascore, imdb_score, views, details_url)
            VALUES (:rank, :title, :description, :year, :duration, :rating, :metascore, :imdb_score, :views, :details_url)
            ON CONFLICT (details_url) DO UPDATE
                SET rank = EXCLUDED.rank,
                    rating = EXCLUDED.rating,
                    metascore = EXCLUDED.metascore,
                    imdb_score = EXCLUDED.imdb_score,
                    views = EXCLUDED.views,
                    last_updated = CURRENT_TIMESTAMP
        """),
        {
            'rank': rank,
            'title': row['title'],
            'description': row['description'],
            'year': year,
            'duration': row['duration'],
            'rating': row['rating'],
            'metascore': metascore,
            'imdb_score': imdb_score,
            'views': row['views'],
            'details_url': row['details_url']
        }
        )

# --- Reload movies with IDs ---
df_movies_db = pd.read_sql("SELECT * FROM movies", engine)
# look up dictionary utilizing zip to pair elements from the sequences description and movie_id position by position
desc_to_id = dict(zip(df_movies_db['description'], df_movies_db['movie_id']))

# --- Utlity funtion to sanitize the directors, writers and stars
def safe_list(val):
    if val is None:
        return []
    if isinstance(val,float) and math.isnan(val):
        return []
    if isinstance(val,list):
        return val
    return []

# --- Insert join tables ---
with engine.begin() as conn:
    for _, row in df_merged.iterrows():

        if row['description'] not in desc_to_id:
            continue

        movie_id = desc_to_id[row['description']]

        # --- Director ---
        if row['director'] and not (isinstance(row['director'], float) and math.isnan(row['director'])):
            pid = get_person_id(row['director'])
            if pid:
                conn.execute(
                    text("""
                        INSERT INTO movie_director (movie_id, person_id)
                        VALUES (:movie_id, :person_id)
                        ON CONFLICT DO NOTHING
                    """),
                    {"movie_id": movie_id, "person_id": pid}
                )

        # --- Writers ---
        for w in safe_list(row['writers']):
            pid = get_person_id(w)
            if pid:
                conn.execute(
                    text("""
                        INSERT INTO movie_writer (movie_id, person_id)
                        VALUES (:movie_id, :person_id)
                        ON CONFLICT DO NOTHING
                    """),
                    {"movie_id": movie_id, "person_id": pid}
                )

        # --- Stars ---
        for s in safe_list(row['stars']):
            pid = get_person_id(s)
            if pid:
                conn.execute(
                    text("""
                        INSERT INTO movie_star (movie_id, person_id)
                        VALUES (:movie_id, :person_id)
                        ON CONFLICT DO NOTHING
                    """),
                    {"movie_id": movie_id, "person_id": pid}
                )


