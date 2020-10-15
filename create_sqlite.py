import numpy as np
import pandas as pd
import time

import memory_profiler as mem_profile

from sqlalchemy import create_engine

nyc_db = create_engine('sqlite:///nyc_db.db', echo=True)
sqlite_connection = nyc_db.connect()
sqlite_table = "nyc_data"

print('total memory (before): {} MB'.format(mem_profile.memory_usage()))

start = time.time()

create_table_sql = """CREATE TABLE nyc_data (
    car_type TEXT, 
    pickup_datetime TEXT, 
    dropoff_datetime TEXT, 
    pulocationid BIGINT, 
    pickup_latitude FLOAT, 
    pickup_longitude FLOAT, 
    dolocationid BIGINT, 
    dropoff_latitude FLOAT, 
    dropoff_longitude FLOAT, 
    trip_duration FLOAT, 
    trip_distance FLOAT, 
    stl_distance FLOAT, 
    payment_type BIGINT, 
    fare_amount FLOAT, 
    tip_amount FLOAT, 
    tolls_amount FLOAT, 
    mta_tax FLOAT, 
    congestion_surcharge BIGINT, 
    improvement_surcharge FLOAT, 
    ehail_fee FLOAT, 
    extra FLOAT, 
    total_amount FLOAT, 
    passenger_count BIGINT, 
    trip_type FLOAT, 
    ratecodeid BIGINT, 
    store_and_fwd_flag TEXT, 
    pickup_year BIGINT, 
    pickup_month BIGINT, 
    pickup_weekday BIGINT, 
    pickup_day TEXT, 
    pickup_hour BIGINT, 
    dropoff_year BIGINT, 
    dropoff_month BIGINT, 
    dropoff_weekday BIGINT, 
    dropoff_day TEXT, 
    dropoff_hour BIGINT
);"""

nyc_db.execute("DROP VIEW IF EXISTS nycdata;")
nyc_db.execute("DROP VIEW IF EXISTS nyc_dataSpeed;")
nyc_db.execute("DROP VIEW IF EXISTS nyc_dataPlus;")
nyc_db.execute("DROP VIEW IF EXISTS nyc_dataPlus_;")
nyc_db.execute("DROP VIEW IF EXISTS nyc_dataRatio;")
 
nyc_db.execute("DROP VIEW IF EXISTS _nyc_dataSpeed;")
nyc_db.execute("DROP VIEW IF EXISTS nyc_location;")
nyc_db.execute("DROP VIEW IF EXISTS nyc_dataWeather;")
nyc_db.execute("DROP VIEW IF EXISTS nyc_data__;")

#################### Table: nyc_data

if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (sqlite_table,)).fetchall():
    nyc_db.execute(create_table_sql)
else:
    print("table {} already exists!".format(sqlite_table))

create_view_nyc_dataPlus_sql = """CREATE VIEW nyc_dataPlus (total_amount, trip_distance,fare_per_mile) AS
   SELECT total_amount, trip_distance, (total_amount/trip_distance) AS fare_per_mile
   FROM nyc_data;"""

#################### Table: nyc_weather

create_table_weather_sql = """CREATE TABLE nyc_weather (
    DATE TEXT, 
    PRCP FLOAT, 
    SNOW FLOAT,
    SNOW_LEVEL BIGINT,
    PRCP_LEVEL BIGINT);""" 

table_name = 'nyc_weather'

if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchall(): 
    nyc_db.execute(create_table_weather_sql)
else:
    print("table {} already exists!".format(table_name))

######################### Table: taxi_lookup

create_table_taxi_sql = """CREATE TABLE taxi_lookup (
    LocationID BIGINT,
    Borough TEXT, 
    Zone TEXT, 
    service_zone TEXT);""" 

table_name = 'taxi_lookup'

if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchall(): 
    nyc_db.execute(create_table_taxi_sql)
else:
    print("table {} already exists!".format(table_name))

################### VIEW: nyc_dataPlus

view_name = 'nyc_dataPlus'
if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall():
    nyc_db.execute(create_view_nyc_dataPlus_sql)
else:
    print("view {} already exists!".format(view_name))
    
################### VIEW: nyc_dataPlus_
    
create_view_nyc_dataPlus_sql_ = """CREATE VIEW nyc_dataPlus_ (car_type, pickup_year, pickup_month, total_amount, trip_distance,fare_per_mile) AS 
    SELECT car_type, pickup_year, pickup_month, total_amount, trip_distance, (total_amount/trip_distance) AS fare_per_mile 
    FROM nyc_data;"""

view_name = 'nyc_dataPlus_'
if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall():
    nyc_db.execute(create_view_nyc_dataPlus_sql_)
else:
    print("view {} already exists!".format(view_name))

################### VIEW: nyc_dataRatio

create_view_nyc_dataRatio_sql = """CREATE VIEW nyc_dataRatio (car_type, dropoff_year, dropoff_month, stl_distance, trip_distance, ratio) AS
   SELECT car_type, dropoff_year, dropoff_month, stl_distance, trip_distance, (stl_distance/trip_distance) AS ratio
   FROM nyc_data
   WHERE stl_distance != 0;"""

view_name = 'nyc_dataRatio'
if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall():
    nyc_db.execute(create_view_nyc_dataRatio_sql)
else:
    print("view {} already exists!".format(view_name))
    
################### VIEW: nyc_dataSpeed

create_view_nyc_dataSpeed_sql = """CREATE VIEW nyc_dataSpeed (trip_distance, trip_duration, driving_speed) AS
   SELECT trip_distance, trip_duration, (trip_distance*60/trip_duration) AS driving_spped
   FROM nyc_data;"""

view_name = 'nyc_dataSpeed'
if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall():
    nyc_db.execute(create_view_nyc_dataSpeed_sql)
else:
    print("view {} already exists!".format(view_name))

################### VIEW: _nyc_dataSpeed

_create_view_nyc_dataSpeed_sql = """CREATE VIEW _nyc_dataSpeed (car_type, pickup_year, pickup_month, trip_distance, trip_duration, driving_speed) AS
   SELECT car_type, pickup_year, pickup_month, trip_distance, trip_duration, (trip_distance*60/trip_duration) AS driving_spped
   FROM nyc_data;"""

view_name = '_nyc_dataSpeed'
if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall():
    nyc_db.execute(_create_view_nyc_dataSpeed_sql)
else:
    print("view {} already exists!".format(view_name))

################### VIEW: nycdata

create_view_nycdata_sql = """CREATE VIEW nycdata (car_type, pickup_datetime, pickup_date, pickup_year, pickup_month, trip_distance, trip_duration, driving_speed) AS
   SELECT car_type, pickup_datetime, CAST(pickup_datetime AS DATE) AS pickup_date, pickup_year, pickup_month, trip_distance, trip_duration, (trip_distance*60/trip_duration) AS driving_speed 
   FROM nyc_data ;"""

view_name = 'nycdata'
if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall():
    nyc_db.execute(create_view_nycdata_sql)
else:
    print("view {} already exists!".format(view_name))

################### VIEW: nyc_dataWeather

create_view_nyc_dataWeather_sql = """CREATE VIEW nyc_dataWeather (car_type, pickup_date, pickup_year, pickup_month, trip_distance, trip_duration, driving_speed, PRCP, SNOW, DATE, SNOW_LEVEL, PRCP_LEVEL) AS
   SELECT car_type, pickup_date, pickup_year, pickup_month, trip_distance, trip_duration, driving_speed, PRCP, SNOW, DATE
          , (SELECT 
              CASE
                WHEN SNOW = 0                     THEN SNOW_LEVEL = 1
                WHEN SNOW > 0        AND SNOW < 2 THEN SNOW_LEVEL = 2
                WHEN SNOW BETWEEN 2 AND 4         THEN SNOW_LEVEL = 3
                WHEN SNOW BETWEEN 4 AND 6         THEN SNOW_LEVEL = 4
                WHEN SNOW BETWEEN 6 AND 8         THEN SNOW_LEVEL = 5
                WHEN SNOW > 8                     THEN SNOW_LEVEL = 6
              END
            FROM nyc_weather
            )
            , (SELECT
                CASE
                  WHEN PRCP = 0                       THEN PRCP_LEVEL = 1
                  WHEN PRCP > 0        AND PRCP < 0.2 THEN PRCP_LEVEL = 2
                  WHEN PRCP BETWEEN .2 AND .4         THEN PRCP_LEVEL = 3
                  WHEN PRCP BETWEEN .4 AND .6         THEN PRCP_LEVEL = 4
                  WHEN PRCP BETWEEN .6 AND .8         THEN PRCP_LEVEL = 5
                  WHEN PRCP > .8                      THEN PRCP_LEVEL = 6
                END
              FROM nyc_weather
              ) 
   FROM nycdata JOIN nyc_weather on nycdata.pickup_date = nyc_weather.DATE;"""

view_name = 'nyc_dataWeather'
if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall():
    nyc_db.execute(create_view_nyc_dataWeather_sql)
else:
    print("view {} already exists!".format(view_name))

################### VIEW: nyc_location

create_view_location_sql = """CREATE view nyc_location 
                              (car_type, pulocationid, dolocationid, puborough, doborough, puzone, dozone) AS
        SELECT car_type, pulocationid, dolocationid, zu.puborough, zo.doborough, zu.puzone, zo.dozone
        FROM nyc_data 
        JOIN taxi_lookup zu ON  nyc_data.pulocationid = zu.locationid 
        JOIN taxi_lookup zo ON nyc_data.dolocationid = zo.locationid;"""

view_name = 'nyc_location'

if not nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,)).fetchall(): 
    nyc_db.execute(create_view_location_sql)
else:
    print("view {} already exists!".format(view_name))
    
#################### INDEX

nyc_db.execute("CREATE INDEX snow ON nyc_weather (DATE, SNOW_LEVEL);")
nyc_db.execute("CREATE INDEX rain ON nyc_weather (DATE, PRCP_LEVEL);")
nyc_db.execute("CREATE INDEX trip ON nyc_data (car_type, pickup_year, pickup_month, payment_type);")
nyc_db.execute("CREATE INDEX location ON nyc_data (car_type, pickup_year, pickup_month, pulocationid, dolocationid);")

###################

nyc_db.execute("PRAGMA table_info(nyc_data);").fetchall()
nyc_db.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
nyc_db.execute("SELECT name FROM sqlite_master WHERE type='view';").fetchall()
