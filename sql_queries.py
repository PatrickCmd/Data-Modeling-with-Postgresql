# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = """
    CREATE TABLE songplays (songplay_id SERIAL PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            user_id int NOT NULL,
                            level varchar,
                            song_id varchar,
                            artist_id varchar,
                            session_id int NOT NULL,
                            location varchar NOT NULL,
                            user_agent text NOT NULL)
"""

user_table_create = """
    CREATE TABLE users (user_id int,
                        first_name varchar,
                        last_name varchar,
                        gender varchar,
                        level varchar)
"""

song_table_create = """
    CREATE TABLE songs (song_id varchar,
                        title varchar,
                        artist_id varchar,
                        year int,
                        duration numeric)
"""

artist_table_create = """
    CREATE TABLE artists (artist_id varchar,
                          name varchar,
                          location varchar,
                          latitude numeric,
                          longtitude numeric)
"""

time_table_create = """
    CREATE TABLE time (start_time TIMESTAMP,
                       hour int,
                       day int,
                       week int,
                       month int,
                       year int,
                       weekday int)
"""

# INSERT RECORDS

songplay_table_insert = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s)
"""

song_table_insert = """
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    VALUES (%s, %s, %s, %s, %s)
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, name, location, latitude, longtitude) 
    VALUES (%s, %s, %s, %s, %s)
"""


time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# FIND SONGS

song_select = """
    SELECT s.song_id, s.artist_id FROM songs s
    JOIN artists t ON s.artist_id = t.artist_id
    WHERE s.title = (%s)
    AND t.name = (%s)
    AND s.duration = (%s)
"""

# FIND USER
user_select = """
    SELECT user_id FROM users
    WHERE user_id = (%s)
"""

# FIND ARTIST
artist_select = """
    SELECT artist_id FROM artists
    WHERE artist_id = (%s)
"""

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
