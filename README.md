***Project: Data Modeling with Postgres***

Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

***Work Done***

I started off by conceptually defining the fact and dimension tables for a star schema. My star schema had the following properties:

Fact Table

*songplays table* - records in log data associated with song plays i.e. records with page NextSong
***Columns*** - songplay_id primary key, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

*users table* - users in the app; 
***Columns*** - userId, firstName, lastName, gender, level

*songs table* - songs in music database; 
***Columns*** - song_id, title, artist_id, year, duration

*artists table* - artists in music database; 
***Columns*** - artist_id, artist_name, artist_location, artist_latitude, artist_longitude

*time table* - timestamps of records in songplays broken down into specific units;
***Columns*** - start_time, hour, day, week, month, year, weekday

The database was already created in the create_tables.py. I then wrote my sql queries to drop the tables if they already exist and created new tables in the database called sparkify. My queries can be seen in the ***sql_queries.py*** file which also includes insert queries. The create_tables.py file basically contains scripts to create the database, drop and create my tables as well as insert records into my tables by importing queries from my sql file. It is crucial to run the create_tables.py file. The test.ipynb allows us to test if the tables have successfully been created in the database.

I inserted records into my tables by writing an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL. This part entailed the Extraction (E), Transformation (T) and Loading (L) or insertion of data or records into our already created tables. In the lessons in classroom, we learned how to insert data manually or from an already existing table using the SELECT statement. Here, I used 'ETL' technique to extract, transform and load the data from json files into the tables to create the database schema.

The etl.ipynb reads and processes a single file from song_data and log_data and loads the data into the tables. This notebook contains information on the ETL process for each of the tables. The etl.py file reads and processes files from song_data and log_data and loads them into the tables. I filled etl.py file based on my work in the ETL notebook.
    
NB: sql_queries.py contains all the sql queries, and is imported into the other files mentioned above.

*create_tables.py drops and creates the tables. it therefore has to be run to reset the tables before each time the ETL scripts are run.*

*You will not be able to run test.ipynb, etl.ipynb, or etl.py until you have run create_tables.py at least once to create the sparkifydb database, which these other files connect to.*

Therefore the project is embedded in the files mentioned above.

***TO-DO:***

1. Run create_tables.py to create database and tables.

2. Run test.ipynb to confirm the creation of tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

3.  Run create_tables.py before running etl.py to reset tables and process the entire datasets respectively.

4. Run test.ipynb to confirm that records were successfully inserted into each table. Remember to rerun create_tables.py to reset your tables before each time you run this notebook.

***I also used this query in the test notebook***
%sql SELECT * FROM songplays WHERE song_id IS NOT NULL and artist_id IS NOT NULL;

To display the row that contains record for both artist_id and song_id.




 
