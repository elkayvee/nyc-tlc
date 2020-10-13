import numpy as np
import pandas as pd
import time
import datetime
from datetime import timedelta

import os

import memory_profiler as mem_profile

from math import cos, sin, asin, sqrt, pi, radians
from sqlalchemy import create_engine
import zipfile

def scrubbed(df):
    df = df[(df['pickup_latitude'] >= -90) & (df['pickup_latitude'] <= 90)
       & (df['pickup_longitude'] >= -90) & (df['pickup_longitude'] <= 90)
       & (df['dropoff_latitude'] >= -90) & (df['dropoff_latitude'] <= 90)
       & (df['dropoff_longitude'] >= -90) & (df['dropoff_longitude'] <= 90) 
       & (df['dropoff_datetime'] >= df['pickup_datetime']) 
       & (df['trip_distance'] >= 0) 
       & (df['trip_duration'] >= 0)
       & (df['stl_distance'] >= 0)
       & (df['passenger_count'] >= 0)]
    return df
  
def straightLineDistance(x):
    # The math module contains a function named radians which converts from degrees to radians. 
    lat2 = x.dropoff_latitude
    lon2 = x.dropoff_longitude
    lat1 = x.pickup_latitude
    lon1 = x.pickup_longitude
    
    if lat1 == 0 or lat2 == 0 or lon1 == 0 or lon2 == 0:
        return 0
    
    lon1 = radians(lon1) 
    lon2 = radians(lon2) 
    lat1 = radians(lat1) 
    lat2 = radians(lat2) 
       
    # Haversine formula  
    dlon = lon2 - lon1  
    dlat = lat2 - lat1 
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
  
    c = 2 * asin(sqrt(a))  
     
    # Radius of earth in kilometers = 6371. Use 3956 for miles 
    r = 3956 
       
    # calculate the result 
    return (c*r)


columns_ = ['car_type' 
            , 'pickup_datetime'
            , 'dropoff_datetime'
            , 'pulocationid' 
            , 'pickup_latitude' 
            , 'pickup_longitude'             
            , 'dolocationid' 
            , 'dropoff_latitude' 
            , 'dropoff_longitude' 
            , 'trip_duration'
            , 'trip_distance' 
            , 'stl_distance' 
            , 'payment_type' 
            , 'fare_amount' 
            , 'tip_amount' 
            , 'tolls_amount' 
            , 'mta_tax' 
            , 'congestion_surcharge' 
            , 'improvement_surcharge' 
            , 'ehail_fee' 
            , 'extra'
            , 'total_amount'
            , 'passenger_count' 
            , 'trip_type' 
            , 'ratecodeid'
            , 'store_and_fwd_flag'
            , 'pickup_year'
            , 'pickup_month'
            , 'pickup_weekday'
            , 'pickup_day'
            , 'pickup_hour'
            , 'dropoff_year'
            , 'dropoff_month'
            , 'dropoff_weekday'
            , 'dropoff_day'
            , 'dropoff_hour'
           ]

print('Total Memory (Before data processing and loading into SQLite the data for green and yellow taxis from 2016 to 2019):{} MB'.\
      format(mem_profile.memory_usage()))

start = time.time()

j= 1

for ftype in ['green', 'yellow']:        
    for year in [2016, 2017, 2018, 2019]:
        for month in range(1,13):

            i = 0
            fname = "nyc_{}_{}-{:02}_.csv".format(ftype, year,month)
            fp = "outnyc_{}_{}-{:02}_.csv".format(ftype, year,month)
            
            df_ = pd.read_csv(filepath_or_buffer = fname, index_col=False) 
            
            try: 
                print("===========================================")                   
                    
                print("Memory: (Before running the file): {} MB".format(mem_profile.memory_usage()))
                print(fname)
                print("===========================================")

                df_.rename(columns={c: c.lower().strip() for c in df_.columns}, inplace=True)
                if 'lpep_dropoff_datetime' in df_.columns:
                    df_.rename(columns={'lpep_dropoff_datetime':'dropoff_datetime','lpep_pickup_datetime':'pickup_datetime'}\
                               , inplace=True)
                if 'tpep_dropoff_datetime' in df_.columns:
                    df_.rename(columns={'tpep_dropoff_datetime':'dropoff_datetime','tpep_pickup_datetime':'pickup_datetime'}\
                               , inplace=True) 

                df_['dropoff_datetime'] = pd.to_datetime(df_['dropoff_datetime'])
                df_['pickup_datetime'] = pd.to_datetime(df_['pickup_datetime'])

                if not 'pulocationid' in df_.columns:
                    df_['pulocationid'] = 0
                    df_['dolocationid'] = 0
                if not 'pickup_latitude' in df_.columns:
                    df_['pickup_latitude']   = 0
                    df_['pickup_longitude']  = 0
                    df_['dropoff_latitude']  = 0
                    df_['dropoff_longitude'] = 0
                if not 'congestion_surcharge' in df_.columns:
                    df_['congestion_surcharge'] = 0
                if not 'ehail_fee' in df_.columns:
                    df_['ehail_fee'] = 0
                if not 'trip_type' in df_.columns:
                    df_['trip_type'] = 0

                df_['car_type'] = ftype
                
                # Derived columns for calculated 

                df_['trip_duration'] = np.round(((df_['dropoff_datetime'] - df_['pickup_datetime'])/timedelta(minutes=1)),2)

                df_['stl_distance'] = 0

                df_['pickup_year'] = df_['pickup_datetime'].dt.year # Extract month
                df_['pickup_month'] = df_['pickup_datetime'].dt.month # Extract month
                df_['pickup_weekday'] = df_['pickup_datetime'].dt.dayofweek # Extract day of week
                df_['pickup_day'] = df_['pickup_datetime'].dt.day_name() # Extract day name (description) of week
                df_['pickup_hour'] = df_['pickup_datetime'].dt.hour # Extract hour
                df_['dropoff_year'] = df_['dropoff_datetime'].dt.year # Extract month
                df_['dropoff_month'] = df_['dropoff_datetime'].dt.month # Extract month
                df_['dropoff_weekday'] = df_['dropoff_datetime'].dt.dayofweek # Extract day of week
                df_['dropoff_day'] = df_['dropoff_datetime'].dt.day_name() # Extract day name (description) of week
                df_['dropoff_hour'] = df_['dropoff_datetime'].dt.hour # Extract hour

                df_.fillna(value=0, inplace = True)
                
                # Calculate straight line distance where latitude and longitude is given
                
                df_['stl_distance'] = df_.apply(straightLineDistance, axis=1)

                df = df_[columns_]
                
                # Data scrubbing
                
                df = scrubbed(df)

                df.to_csv(fp, index=False, header=True, columns=columns_)

            except Exception as e:
                print("Exception message for: ", fname, "\n\t", str(e))
                continue

print('Total Memory (After data processing of ALL the data for green and yellow taxis from 2016 to 2019): {} MB'.\
      format(mem_profile.memory_usage()))

print("The total time taken to data process complete green and yellow trip dataset: {:2f} minutes".\
      format((time.time() - start)/60))