import numpy as np
import pandas as pd
import time
import datetime
from datetime import timedelta

import urllib.request

import os
from os import environ

import memory_profiler as mem_profile

from math import cos, sin, asin, sqrt, pi, radians
from sqlalchemy import create_engine
import zipfile

# Download the Trip Record Data from the urls to local storage
# Input file: url
# Output file: "nyc_{}_{}-{:02}.csv".format(ftype, year,month)

print('Memory (Before): {} MB'.format(mem_profile.memory_usage()))

start = time.time()

for ftype in ['green', 'yellow', 'fhv', 'fhvhv']:
    for year in [2016,2017,2018,2019]:
        for month in range(1,13):
            print("Trip Data being downloaded for Year: {}, month: {:02}".format(year, month))
            print("======================")
            print("Memory (Before): file: {}_tripdata_{}-{:02}.csv :{} MB".format(ftype, year, month, mem_profile.memory_usage()))
            try:
                urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/trip+data/"+ \
                                           "{}_tripdata_{}-{:02}.csv".format(ftype, year, month), 
                                           "nyc_{}_{}-{:02}.csv".format(ftype, year,month))
            except Exception as e:
                print("Except: ", fname)
                print(str(e))
                continue
            print("Memory (After): file: {}_tripdata_{}-{:02}.csv :{} MB".format(ftype, year, month, mem_profile.memory_usage()))

print('Total Memory (After download of ALL the data): {} MB'.format(mem_profile.memory_usage()))

print("The total time taken to load complete trip dataset: {:2f} minutes".format((time.time() - start)/60))

# Check number of columns in the header line of each file downloaded, i.e., Check for schema change over time:

# Input file: nyc_{}_{}-{:02}.csv".format(ftype, year,month)


print()

start = time.time()

df_columns_prev = ""
fname_prev = ""
for ftype in ['green', 'yellow', 'fhv', 'fhvhv']: 
    print(ftype)
    print("======")
    i = 0
    for year in [2016,2017,2018,2019]:
        for month in range(1,13):
            fname = "nyc_{}_{}-{:02}.csv".format(ftype, year,month)
            if os.path.isfile(fname):
                df = pd.read_csv(fname, nrows=2)
                df.rename(columns={c: c.lower().strip() for c in df.columns}, inplace=True)
                cols = len(df.columns)
                if i != 0 and cols != cols_prev:
                    diff_col  = df.columns.difference(df_columns_prev.columns)
                    diff_col_ = df_columns_prev.columns.difference(df.columns)
                    if len(diff_col):
                        print(fname, " - ", fname_prev)
                        print("---------------------------------------------")
                        print("Fields added:\n", df.columns.difference(df_columns_prev.columns))
                        print()

                    if len(diff_col_):
                        print(fname_prev, " - ", fname)
                        print("---------------------------------------------")
                        print("Fields removed:\n", df_columns_prev.columns.difference(df.columns))
                        print()
                        
                    print()
                fname_prev = fname
                df_columns_prev = df
                cols_prev = cols
                i = 1
print()
print("The total time to run header column check script: {:.2f} minutes".format((time.time() - start)/60))  
print()
print('Memory (After running header column check script): {} MB'.format(mem_profile.memory_usage()))

# Now check the number of fields in the complete file to make sure there are no records with mismatched number of fields within the file:

# Input file: nyc_{}_{}-{:02}.csv".format(ftype, year,month)

start = time.time()

for ftype in ['green', 'yellow']: 
    for year in [2016, 2017, 2018, 2019]:
        for month in range(1,13):
            j = 0
            j_prev = 0
            fname = "nyc_{}_{}-{:02}.csv".format(ftype, year,month)
            fp = f'out_{ftype}_{year}-{month:02}.txt'
            
            print("file:", fname)
            
            with open(fp, 'r') as f:
                lines = f.readlines()
                
                for i in range(len(lines)):
                    j = lines[i].rstrip('\n')
#                     print(j)
                    if i == 0:
                        print("\tThe number of columns:", j)
                        j_prev = j
                        continue
                    if j != j_prev:
                        print("\tline:", i, "\t", j_prev, "\t", j)
                    j_prev = j

print("total time taken for column check in each file:", np.round((time.time() - start)/60, 2))      
print()
print('Memory (After running column check in each file): {} MB'.format(mem_profile.memory_usage()))

# The same can be accomplished by running the bash awk script to check if the files have same number of fields in each record:

# start = time.time()
# for ftype in ['green', 'yellow']: 
#     for year in [2016, 2017, 2018, 2019]:
#         for month in range(1,13):
            
#             fname = f'nyc_{ftype}_{year}-{month:02}_.csv'
#             fp = f'out_{ftype}_{year}-{month:02}.txt'

#             environ['fname'] = fname
#             environ['fp'] = fp
            
#             !awk -F',' '{print NF;}' "$fname" > "$fp"
            
# print("total time taken to run awk script:", np.round((time.time() - start)/60, 2))   
# print()
# print('Memory (After running column check script): {} MB'.format(mem_profile.memory_usage()))

# from 2016-01 through 2016-06 the green taxi data has Lpep_dropoff_datetime as one of the columns and lpep_dropoff_time later
# the number of fields (read schema) changed a few times over the years between 2016 and 2019
# FHV and FHVHV data files capture only following pertinent information:
    # Dispatching_base_num
    # Pickup_datetime
    # DropOff_datetime
    # PULocationID 
    # DOLocationID
    # SR_Flag
# for FHV data files & FHVHV files have additional field for:
    # Hvfhs_license_num
    
# None of these convey any information regarding the payment or driving behaviors. So we cannot include them in our analysis queries.

# run sed script to get rid of the " characters and any potential blank lines
# Input file: nyc_{}_{}-{:02}.csv".format(ftype, year,month)
# Output file: "nyc_{}_{}-{:02}_.csv".format(ftype, year,month)

start = time.time()
for ftype in ['yellow', 'green', 'fhv', 'fhvhv']: 
    for year in [2016,2017,2018,2019]:
        for month in range(1,13):           
            fname = "nyc_{}_{}-{:02}.csv".format(ftype, year,month)
            fname_ = "nyc_{}_{}-{:02}_.csv".format(ftype, year,month)
            if os.path.isfile(fname):
                !sed -e 's/\"//g' -e '/^$/d' $fname > $fname_
 
print("total time taken to run sed scripts:", np.round((time.time() - start)/60, 2))


# For better Manageability of the data files (some ofthem are huge - between 2.5 to 3 GB), we split them  into chunks of 500000 
# when reading into dataframe to manipulate and load the SQLite database

# ===========================================
# Input file: "nyc_{}_{}-{:02}_.csv".format(ftype, year,month)
# Output file: "outnyc_{}_{}-{:02}_.csv".format(ftype, year,month)

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

# ===========================================

start = time.time()

# Split very large files into manageable chunks 
# Input file: "outnyc_{}_{}-{:02}_.csv".format(ftype, year,month)
# Output file: "_outnyc_{}_{}.csv_{:02}".format(file.split('_')[1], file.split('_')[2], i)

for file in sorted(glob.glob("outnyc_*.csv")):
    print(file)
    print("-----------------")
    i = 1
    with open(file, 'r') as src:
        df = pd.read_csv(src, index_col = False, header=None, names= columns_, chunksize=500000)
        for chunk in df:
            f = "_outnyc_{}_{}.csv_{:02}".format(file.split('_')[1], file.split('_')[2], i)
            print("\t", f)
            chunk.to_csv(f, index=False, header=True)
            i += 1
    print()       
print('Total Memory (after splitting taxi data files into multiple manageable files): {} MB'.\
      format(mem_profile.memory_usage()))

print("The total time taken to split taxi data files into multiple files for trip dataset: {:2f} minutes".\
      format((time.time() - start)/60))

# load taxi_zone data
df_zone = pd.read_csv("taxi+_zone_lookup.csv")

# load monthly CPI data
df_cpi = pd.read_csv("cpi.csv")
