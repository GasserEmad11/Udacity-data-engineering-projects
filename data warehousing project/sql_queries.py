import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """ DROP TABLE IF EXISTS staging_events"""
staging_songs_table_drop = """ DROP TABLE IF EXISTS staging_songs """
songplay_table_drop = """ DROP TABLE IF EXISTS songplay """
user_table_drop = """DROP TABLE IF EXISTS users """
song_table_drop = """ DROP TABLE IF EXISTS song"""
artist_table_drop = """ DROP TABLE IF EXISTS artist"""
time_table_drop = """ DROP TABLE IF EXISTS time """

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist text PRIMARY KEY ,
auth      text,
firstname text,
gender    text,
iteminsession int,
lastname  text,
length    numeric,
level     text,
location  text,
method    text,
page      text,
registration numeric,
session_id int,
song      text,
status    int,
ts        bigint,
useragent text,
user_id   int);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(song_id varchar  PRIMARY KEY,
artist_id        varchar,
artist_lattitude numeric,
artist_longitude numeric,
artist_location  text,
artist_name      text,
num_songs        int ,
title            text,
duration         numeric,
year             int
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(songplay_id  int IDENTITY (0,1) PRIMARY KEY ,
start_time     timestamp distkey ,
user_id        int,
level          text,
song_id        varchar,
artist_id      varchar,
session_id     int,
location       text,
useragent      text

);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (  user_id int PRIMARY KEY,
        firstname         text,
        lastname          text,
        gender            text,
        level             text)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song(song_id varchar PRIMARY KEY NOT NULL,
        title       varchar ,
        artist_id   varchar ,
        year        int,
        duration    numeric)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS aritst(artist_id varchar PRIMARY KEY NOT NULL,
        name       text,
        location   text,
        lattitude  numeric,
        longitude  numeric)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time( start_time  timestamp not null distkey  PRIMARY KEY,
        hour        int not null,
        day         int not null,
        week        int not null,
        month       int not null,
        year        int not null,
        weekday     varchar not null)
""")

# STAGING TABLES


staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {}
                          region 'us-west-2';
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto'
                          region 'us-west-2';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + e.ts * interval '0.001 seconds' as start_time,
        e.user_id as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.session_id as session_id,
        e.location as location ,
        e.useragent as useragent
FROM staging_events  e,staging_songs s
WHERE e.song=s.title AND e.artist=s.artist_name AND e.length=s.duartion
AND e.page='NextSong'



""")

user_table_insert = ("""INSERT INTO users (user_id, firstname, lastname, gender, level)
SELECT DISTINCT e.user_id as user_id,
                e.firstname as firstname,
                e.lastname as lastname ,
                e.gender as gender,
                e.level as level
FROM staging_events e 

""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT s.song_id as song_id ,
                s.title as title,
                s.artist_id as artist_id ,
                s.year as year,
                s.duration as duration
FROM staging_songs s
""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT s.artist_id as artist_id ,
                s.artist_name as name ,
                s.location as location,
                s.artist_lattitude as lattitude ,
                s.artist_longitude as longitude 
FROM staging_songs s
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT s.startime ,
EXTRACT (hour from s.startime),
EXTRACT (day from s.startime),
EXTRACT (week from s.startime),
EXTRACT (month from s.startime),
EXTRACT (year from s.startime),
EXTRACT(weekday from s.startime)
FROM songplay s
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
