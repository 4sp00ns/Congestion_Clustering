# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 12:32:42 2020

@author: Duncan
"""
import numpy as np
import ATXxmlparse
import sklearn.cluster as c
#classpath = ":".join(pyspark.classpath_jars())
def create_spark():
    spark = SparkSession.builder.getOrCreate()
    return spark
def getTripData(path):
    tripData = ATXxmlparse.getTrips(path)
    return tripData
def allOD(tripData):
    allOD = []
    for t in tripData:
        allOD.append([t[1],t[2]])
        allOD.append([t[3],t[4]])
    return allOD 
def run_kmeans(trip_locations):
    data_array = np.asarray(trip_locations)
    #trip_loc.show()
    #data_array = np.array(trip_loc.collect())
    #for i in range (0,100,25):
    maxiter = 300 + 30*50
    k_means_data = c.k_means(data_array, 250, n_init = 50+10, max_iter = maxiter, verbose = True)
    centroid_arr = k_means_data[0]
    #txtout = np.asarray(latlon)
    #np.savetxt('testout_weight.csv', txtout, delimiter=',')
    return k_means_data

def centroid_2_location(latlon, loc_df):
    loc_array = np.array(locations_df.collect())
    #for pudo in latlon: