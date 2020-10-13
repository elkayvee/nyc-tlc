# nyc-tlc
Analysis of public -tlc trip data data

 normalized the data wherever there was need for comparing the trend, for example, between "green" and "yellow" taxis over period of time, by subtracting the min of the feature and dividing by the difference between max and min of the numeric feature. I created new features: trip_duration, day, hour, weekday each for pickup and dropoff times, straight line distance between the latitude and longitude of the pickup and dropoff location (which is basically first 6 months of 2016 only).

The FHV and FHVHV data files are huge but basically contain no payment information. As such almost all the analyses questions (since they pertain to payments) do not apply to them and hence are excluded from analyses.

Used matplotlib for visualizing the plots and trends comparisons over time, for example. It is easy, integated into python and jupyter notebook workflows and familiarity with them. Besides, the analysis lent itself much more readily and handily to that kind of visualization. Hence the choice!!

Most of the analysis undertaken is indicative in the sense they inform the direction. The values from each lend themselves to change easily were sightly different interpretations were to be made for presence or absence of "certain kind of values", for example. The case in point, the presence of large negative values records can either be treated as 'voided" transactions OR also as "erroneous" data entry transactions. The change to exclude one from the other is easy and generally not very material (except in the final answer) in investigation except for specific output values. They still inform the output trend easily.

Different variables/Features from different csv files: 
To start with, the data files, as they exist on the public url have a lot of data issues with them:

To name a few:

a) inconsistency between the column header names (specifically, Lpep_dropoff_datetime appears as lpep_dropoff_datetime after a few months. This causes the misalignment of pandas dataframe since the columns are misaligned after reading in. This causes a lot of issues if not minutely taken care of. Very easy to miss this. In fact it came to attention after the behvior was observed that was inconsistent with expectations.

b) a closely related but much more obvious issue - schema "drift", if you will, over a period of time. That is to ay, the number of columns in the various data files for the same type of record varied over the years. So, the schema changed from 21 fields to 19 fields to 20 fields etc. This, once again, caused significant problems when processing the data

c) inconsistency between "green" and "yellow" taxi records on the one hand and fhv and fhvhv taxi records on the other. The latter category of vehicles do not contain any payment related information of any kind. As such they had to be excluded from all ther analyses pertaining to payments, even though those files contain rich ride frequency behaviors and ride origin and destination related behaviors.

d) the biggest challenge ariswe owing to the data files sizes - especially for individual files over 2 GB size. The pandas dataframe could not handle those despite increasing the memory size. After repeated crashes (in the initial phase of the dta cleaning - coming up with the architecture to counter releated crashing of instance was the norm), as a conscious strategy it was decided to read the csv data files in chunks, scrub the data, develop a set of common minimum features, write them into smaller size chunks into multiple files (in effect splitting the original data file) and then reading them into a database (in our case SQLite that comes standard with python). So the analyses transformed from working with data in Pandas dataframe to working with advanced SQL queries (more advanced features like CTEs and Widow functions etc.) to adapt to the needs.

e) the consideration of ALL data from 2016 to 2019 lends this to truly a big data challenge. The need to store the data in the SQLite storage makes the SQL queries, however optimized, to retrieved them computationally and performance-wise challenging, what with few queries running well over half an hour (with most on an average running for 16 to 17 minutes).

With these in mind,

Decided to drop VendorID - two values that are not at all material to analyses/investigations

converted timestamp fields 'lpep_pickup_datetime' & 'lpep_dropoff_datetime' into datetime data type, converted to lower case for pandas dataframe compatibility from one csv files to next as the schema changes; read each individual csv file, processed the columns names into lower case, extracted common pool of features (23 of them) from each and then split the files into multiple manageable files to write to SQLite database for analyses to become essentially complex SQL queries with CTE_s (common table expression) and _Window functions!

Driver entered/changeable fields like passenger_counts is also disregarded from considerations (though not dropped) for lack of reliability.

store_and_fwd_flag - is dropped as does not add to variance of data and deemed not material.

RateCodeID also does not add variation to data although is retained. As are all the continuous variables like fare amount etc.

PULocationID and DOLocationID do not lend themselves to straight line distance calculations as the areas are essentially polygons of points instead of single lat and lon values. It is technically possible to approximate to some lat/lon but the quantumn of effort is very high for the relative output of accuracy resultant that are low. So the effort is discontinued.

Most reliable amount fields pertain to credit card transactions, mostly because they are convenient and do not lend themselfves to easy manipulation (like changeable field passenger count, e.g.). While most 0 tip aounts seem to be from cash transactions, it is erroneous to assume that all cash transactions result in 0 tips. It just means that no one bothers to record the tip amount if it is paid in cash because that is strictly voluntary effort to record an additional workflow step.

There are quite a few trip records where the amounts are essentially negative. It is assumed that those are some sort of voided transactions. This can be changed easily with a different interpretations. For now they are just treated as voided transactions.

It is possible to reconcile some of the charges like 'ehail_fee' , 'mta_tax' or 'improvement_surcharge', 'congestion_surcharge" etc. etc. with some of the amount fields populated but that will be lot of effort and worth only if the specific analyses require them. In the current purview of data they are not.
