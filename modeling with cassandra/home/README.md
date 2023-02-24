    This project aims to devolp a databse for the stratup sparkify to help ease the analytical processes.
the dataset availible is a subset of million songs data provided in json format.Using pandas to read the data and allow for the various different manipulations needed for the data to fit the intended database design.
    
    
    Using python and various libraries to develop a script that enables the creation of the sparkify database and automate the insertion of intended data to the database.The steps needed are as follows:
1-run the create_tables.py program which initiates the database using the sql_queries.py file 
2-run the etl.py program that insert the data into the created database using the insert queries written in the sql_queries.py
3-The test notebook is used to verify and check the correct insertion of data and to look at the design of the database
   
   
    A star schema was chosen for the design of the database due its fast response and lightwieght which makes it ideal for the volume, size and datatype of the data.
It consists of the fact table(songplay) and four dimensions table(songs,users,artist,time).
two etl piplines were used in the etl.py file:
1-Process_log_data to process the log_data file and assert it to the database.
2-Process_song_data which performs the same as the latter but using the songs_data file.

    After following the previous instructions for creating and running the database, it should facilitate the analysis of the data and provide excellent use to the company's anayltics department.