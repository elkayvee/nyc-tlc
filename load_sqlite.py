import numpy as np
import pandas as pd
import time

import glob

from sqlalchemy import create_engine

nyc_db = create_engine('sqlite:///nyc_db.db', echo=True)
sqlite_connection = nyc_db.connect()
sqlite_table = "nyc_data"

def snow_level(x):
    if x == 0:
        return 1
    elif x < 2:
        return 2
    elif x < 4:
        return 3
    elif x < 6:
        return 4
    elif x < 8:
        return 5
    else:
        return 6
    
def prcp_level(x):
    if x == 0:
        return 1
    elif x < .2:
        return 2
    elif x < .4:
        return 3
    elif x < .6:
        return 4
    elif x < .8:
        return 5
    else:
        return 6
    
start = time.time()

for filename in sorted(glob.glob("_outnyc_*.csv_*")):
    
    print(filename)
    print("-----------------------------") 
    
    df = pd.read_csv(filename, index_col = False)

    df.to_sql(sqlite_table, nyc_db, if_exists='append', index=False)

    print()
    
print('Total Memory (after loading into SQLite of ALL the trip data from 2016 to 2019): {} MB'.format(mem_profile.memory_usage()))

print("The total time taken to load into SQLite complete green and yellow trip dataset: {:2f} minutes".format((time.time() - start)/60))

df = pd.read_csv("taxi+_zone_lookup.csv" , index_col = False)
df.to_sql('taxi_lookup', nyc_db, if_exists='append', index=False)

df_ = pd.read_csv("US - NY Central Park Weather Station Summary.csv", index_col = False)
_df = df_[['DATE', 'PRCP', 'SNOW']]
_df['SNOW_LEVEL'] = _df['SNOW'].apply(snow_level)
_df['PRCP_LEVEL'] = _df['PRCP'].apply(prcp_level)
_df.to_sql('nyc_weather', nyc_db, if_exists='append', index=False)
