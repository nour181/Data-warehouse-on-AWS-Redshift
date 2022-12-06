# Data-warehouse-on-AWS-Redshift

### Project Overview:

Sparkify is a music streaming startup, Sparkify decided to move its work to cloud (AWS), as a data engineer: my role is to move data from staging S3 to AWS Redshift, data resides on S3 Buckets on AWS on json files




### Datasets:

we are working on two datasets:
1) Log Data: data contains loging activities of Sparkify users
2) Song Data: contains informations and metadata about songs and thier artists
Note about song data files: json files are partitioned by the first three letters of each song's track ID


### Schemas:

we will create 2 staging tables(staging_events - staging_songs)that we will import data on it from the S3 bucket, after that we will create our start schema that contains 5 tables(4 dim tables, 3 fact table):
1) songplays table:
songplay_id, start_time, user_id, level, song_id, artist_id ,session_id, location, user_agent

2) dim_users: 
user_id, first_name, last_name, gender, level

3) dim_songs:
song_id, title, artist_id, year, duration

4) dim_artists:
artist_id, name, location, lattitude, longitude
    
5) dim_time:
start_time, hour, day, week, month, year, weekday


### Project Details and Process:

first we will create our tables on sql_queries.py file, after that we will run create_tables.py to create our tables, staging tables and drop tables if were created in last moments, then we will run etl.py to start our ETL Process and import data from our S3 Bucket to Redshift cluster and make our analytics 


### Project Files:

1) sql_queries.py: contains sql queries for droping existing tables, creating staging and star schema tables
2) create_tables.py: include functions to create staging tables and star schema tables and drop previous tables
3) etl.py: contains functions to load data from s3 bucket into staging tables and inserting data into the five tables
