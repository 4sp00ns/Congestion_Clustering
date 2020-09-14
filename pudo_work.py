# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 12:32:42 2020

@author: Duncan
"""
import utm
import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, IntegerType
import sklearn.cluster as c
#classpath = ":".join(pyspark.classpath_jars())
def create_spark():
    spark = SparkSession.builder.getOrCreate()
    return spark
def get_files(config):
    trip_df = spark.read.option("header","True").csv(r'F:\POLARIS\polaris-development\data\Bloomington\bloomington11\Bloomington_trips.csv')
    location_df = spark.read.option("header","True").csv(r'F:\POLARIS\polaris-development\data\Bloomington\bloomington11\Bloomington_locations.csv')
    #print(trip_df.dtypes)

    trip_locations = trip_df.select('origin')
    trip_locations = trip_locations.withColumnRenamed('origin','location')
    trip_locations = trip_locations.union(trip_df.select('destination'))
    return trip_df, location_df

#trip_typed = trip_locations.withColumn('location2',trip_locations.location.cast(IntegerType())).drop(f.col('location'))

# print(trip_locations.count())
#print(trip_locations.dtypes)
#print(trip_locations.filter(f.col('location') == 1512).count())
#print(location_df.dtypes)
#trip_locations.show()
#weight_df = trip_locations#.groupby('location').agg(f.count('location')).alias('count')

#trip_loc = trip_typed.join(location_df,\
#                           location_df.location == trip_typed.location2)\
#                           .select('x','y').distinct()

#trip_loc = trip_loc.join(weight_df,\
#                         weight_df.location == trip_loc.location2)\
#                         .select('x','y','weight')

latlon = []
def run_kmeans():
    weight_array = np.array(trip_locations.collect())
    #trip_loc.show()
    data_array = np.array(trip_loc.collect())
    for i in range (0,100,25):
        maxiter = 300 + 30*i
        k_means_data = c.k_means(data_array, 250, n_init = i+10, max_iter = maxiter, verbose = False)
        centroid_arr = k_means_data[0]
        for j in centroid_arr:
            ll = utm.to_latlon(j[0],j[1],16, 'N')
            latlon.append([ll[0],ll[1],i])
    txtout = np.asarray(latlon)
    np.savetxt('testout_weight.csv', txtout, delimiter=',')
    return latlon

def centroid_2_location(latlon, loc_df):
    loc_array = np.array(locations_df.collect())
    for pudo in latlon:
        

