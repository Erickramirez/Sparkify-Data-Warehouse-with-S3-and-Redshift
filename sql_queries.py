import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# create staging_events table, using event_id as primary key
staging_events_table_create= (
    """
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id INT IDENTITY(0,1),
        artist_name VARCHAR(255),
        auth VARCHAR(50),
        user_first_name VARCHAR(255),
        user_gender  VARCHAR(1),
        item_in_session INTEGER,
        user_last_name VARCHAR(255),
        song_length DOUBLE PRECISION, 
        user_level VARCHAR(50),
        location VARCHAR(255),
        method VARCHAR(25),
        page VARCHAR(50),
        registration VARCHAR(50),
        session_id BIGINT,
        song_title VARCHAR(255),
        status INTEGER, 
        ts VARCHAR(50),
        user_agent TEXT,
        user_id VARCHAR(100),
    PRIMARY KEY (event_id))
    """)

# create staging_songs_table_create table, using song_id as primary key
staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs(
        song_id VARCHAR(50),
        num_songs INTEGER,
        artist_id VARCHAR(100),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        title VARCHAR(255),
        duration DOUBLE PRECISION,
        year INTEGER,
        PRIMARY KEY (song_id))
    """)

# create songplays table, using songplay_id as primary key
#    using song_id as Redshift Distribution Keys. start_time and  session_id as Sort Keys
songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT IDENTITY(0,1),
        start_time TIMESTAMP,
        user_id VARCHAR(50),
        level VARCHAR(50),
        song_id VARCHAR(50),
        artist_id VARCHAR(100),
        session_id BIGINT,
        location VARCHAR(255),
        user_agent TEXT,
    PRIMARY KEY (songplay_id))
    DISTKEY (song_id)
    SORTKEY (start_time, session_id);
    """)

# create users table, using user_id as primary key
#    Adding foreign key constraint to songplays ads related to this table using user_id column
user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR(50) NOT NULL,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender VARCHAR(1),
        level VARCHAR(50),
    PRIMARY KEY (user_id))
    DISTSTYLE ALL;
    ALTER TABLE songplays ADD CONSTRAINT FK_users FOREIGN KEY (user_id) REFERENCES users(user_id);
    """)

# create songs table, using song_id as primary key
#    Adding foreign key constraint to songplays ads related to this table using user_id column
song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(50),
        title VARCHAR(255),
        artist_id VARCHAR(100) NOT NULL,
        year INTEGER,
        duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
    DISTSTYLE ALL;
    ALTER TABLE songplays ADD CONSTRAINT FK_songs FOREIGN KEY (song_id) REFERENCES songs(song_id);
    """)

# create artists table, using artist_id as primary key
#    Adding foreign key constraint to songplays ads related to this table using artist_id column
artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(100),
        name VARCHAR(255),
        location VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
    DISTSTYLE ALL;
    ALTER TABLE songplays ADD CONSTRAINT FK_artists FOREIGN KEY (artist_id) REFERENCES artists(artist_id);
    """)
# create time table, using start_time as primary key
time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER,
    PRIMARY KEY (start_time))
    """)

# STAGING TABLES
# Load data from S3 into staging_events table.
# Note: delete operation added in order to remove previous data in the staging table
staging_events_copy = (
    """
        DELETE FROM staging_events;
        copy staging_events 
        from {}
        iam_role {}
        format json as {};
    """).format(config.get('S3','LOG_DATA'), 
                config.get('IAM_ROLE', 'ARN'), 
                config.get('S3','LOG_JSONPATH'))

# Load data from S3 into staging_songs table.
# Note: delete operation added in order to remove previous data in the staging table
staging_songs_copy = (
    """
        DELETE FROM staging_songs;
        copy staging_songs 
        from {} 
        iam_role {}
        format json as 'auto';
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
#Get only data of NextSong pages
songplay_table_insert = (
    """
        INSERT INTO songplays (
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent) 
        SELECT  
            TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
            e.user_id, 
            e.user_level, 
            s.song_id,
            a.artist_id, 
            e.session_id,
            e.location, 
            e.user_agent
        FROM staging_events e
            INNER JOIN artists a
                ON a.name = e.artist_name 
            INNER JOIN songs s
                ON s.title  = e.song_title
                AND s.duration = e.song_length
                AND s.artist_id = a.artist_id
        WHERE e.page = 'NextSong'
    """)


user_table_insert = (
    """
        INSERT INTO users (
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level)
        SELECT DISTINCT  
            user_id, 
            user_first_name, 
            user_last_name, 
            user_gender, 
            user_level
        FROM staging_events
        WHERE page = 'NextSong'
    """)

song_table_insert = (
    """
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        ) 
        SELECT DISTINCT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration 
        FROM staging_songs
    """)

artist_table_insert = (
    """
        INSERT INTO artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude
        )
        SELECT DISTINCT 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
        FROM staging_songs
    """)

time_table_insert = (
    """
        insert into time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT DISTINCT 
            start_time,
            extract(hour from start_time) as hour,
            extract(day from start_time) as day,
            extract(week from start_time) as week,
            extract(month from start_time) as month,
            extract(year from start_time) as year,
            extract(weekday from start_time) as weekday
        from songplays 

    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]