# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id int, start_time int, user_id int, level int, song_id varchar, artist_id int, session_id int, location varchar, user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id int, first_name varchar, last_name varchar, gender varchar, level int);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar, title varchar, artist_id varchar, year int, duration int);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id int, name varchar, location varchar, latitude varchar, longitude varchar);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time int, hour int, day varchar, week int, month int, year int, weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(songplay_id , start_time , user_id , level , song_id , artist_id , session_id , location , user_agent )\
values (%s,%s,%s,%s,%s,%s,%s,%s,%s);


""")

user_table_insert = ("""INSERT INTO songs(song_id , title , artist_id , year , duration )\
values (%s,%s,%s,%s,%s);
""")

song_table_insert = ("""INSERT INTO users(user_id , first_name , last_name , gender , level )\
values (%s,%s,%s,%s,%s);
""")

artist_table_insert = ("""INSERT INTO artists (artist_id , name , location , latitude , longitude )\
values (%s,%s,%s,%s,%s);
""")


time_table_insert = ("""INSERT INTO  time (start_time , hour , day , week , month , year , weekday 
)\
values (%s,%s,%s,%s,%s,%s,%s)
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]