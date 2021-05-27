# Project Data Modeling With Postgres
This is a data modeling project created under Data Modeling With Postgres for [Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)

The project is to about building an ETL pipeline from the given datasets and also model a data schema which will be used in building optimized queries for analysis
on song plays by users of Sparkify music streaming app. The description of the datasets below provide a berief explanation.

## Song Dataset
The first dataset is a Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

## Log Dataset
The second dataset consists of log files in JSON format based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

## Schema for Song Play Analysis
Using the song and log datasets in the data folder, below is a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
2. **users** - users in the app
    - user_id, first_name, last_name, gender, level
3. **songs** - songs in music database
    - song_id, title, artist_id, year, duration
4. **artists** - artists in music database
    - artist_id, name, location, latitude, longitude
5. **time** - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday


## Contents

- **sql_queriespy**: The module contains all sql queries for droping, creating tables and also queries for retrieving data from tables and are imported in the scripts below.
- **create_tables.py**: The module drops and create tables. Run this module file to reset tables before each time you run the ETL module script.
- **etl.py**: This module is the ETL script. Reads and processes files from song_data and log_data and loads them into respective tables.

## How to run
On the terminal run the following scripts in the order shown below

```
>>>python create_tables.py
>>>python etl.py
```
