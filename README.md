IMDb Top 250 Movie Rating Aggregator 🎬
Overview

This project builds a normalized relational database of IMDb’s Top 250 Movies, combining movie metadata, ratings, and casting relationships through a scheduled web-scraping pipeline.

The solution uses Docker, Selenium, BeautifulSoup, Pandas, and PostgreSQL, and is designed to run automatically on a weekly basis using cron.

Data Source

Website: IMDb

Page scraped:
https://www.imdb.com/search/title/?groups=top_250&count=250&sort=user_rating,desc

Dataset: IMDb Top 250 Movies (ranked by user rating)

Pipeline Architecture

Extract → Transform → Load (ETL)

1. Extract

Scrapes the IMDb Top 250 movies page using Selenium (to handle dynamic content).

Extracts movie-level data:

- rank
- title
- description
- year
- duration
- rating
- metascore
- imdb_score
- views
- details_url

Visits each movie’s detail page to extract:

- director
- writers
- stars

Note:
All extracted fields are wrapped in try/except blocks to gracefully handle missing or inconsistent HTML elements.

2. Transform

- Converts scraped data into pandas DataFrames.
- Merges movie-level and detail-level data using the movie description as a join key.
- Normalizes multi-valued fields:
- Writers and stars are stored as lists.
- Applies helper functions to enforce schema correctness:
- Converts invalid or missing numeric values to NULL
- Prevents invalid inserts into typed database columns

3. Load

- Loads data into a normalized PostgreSQL schema.

- Uses UPSERT logic (ON CONFLICT) to avoid duplicate movies and update dynamic fields.

- Establishes many-to-many relationships between movies and people.

Database Schema
Tables
- movies: Stores core movie information.
  
Column	Type
movie_id	SERIAL (PK)
rank	INT
title	VARCHAR
description	TEXT
year	INT
duration	VARCHAR
rating	VARCHAR
metascore	INT
imdb_score	DECIMAL(3,1)
views	VARCHAR
details_url	VARCHAR (UNIQUE)
last_updated	TIMESTAMP

- people: Stores unique individuals (directors, writers, stars).

Column	Type
person_id	SERIAL (PK)
name	VARCHAR

Join Tables
- movie_director (movie_id, person_id)
- movie_writer (movie_id, person_id)
- movie_star (movie_id, person_id)
Each join table uses a composite primary key to prevent duplicate relationships.

Docker Setup
The solution runs using Docker Compose and creates three containers:
- PostgreSQL (Username: postgres , Password: docker)
- pgAdmin (URL: http://localhost:8081 , Username: duser@domain.com, Password: SuperSecret)

Scraper Container
- Runs the Python scraping pipeline
- Executes automatically via cron

How to Run
Prerequisites:
- Docker installed
- Docker Compose installed

Start the pipeline
- docker-compose up --build

This will:
- Build the scraper image
- Start PostgreSQL and pgAdmin
- Schedule the scraper job

Scheduling:
- The scraper is triggered via cron: runs once per week
- Default schedule: Sunday at 00:00

Python Environment
- Python version: 3.9

Key libraries:
- selenium
- beautifulsoup4
- pandas
- sqlalchemy
- psycopg2

Design Considerations
- Idempotent pipeline: Re-running the scraper does not create duplicates.
- UPSERT strategy ensures dynamic fields (ratings, views) stay fresh.
- Normalized schema prevents duplicated people.
- Defensive scraping prevents pipeline crashes due to missing fields.
- Efficient lookups via in-memory dictionaries avoid excessive database queries.

Future Improvements
- Use details_url instead of description as the merge key.
- Convert views to a numeric type.
- Track historical rating changes (slowly changing dimensions).
- Add logging and monitoring.
- Add retry/backoff logic for scraping failures.

Author

Built as a data engineering exercise focused on real-world ETL, scraping, and relational modeling best practices.
