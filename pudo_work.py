# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 12:32:42 2020

@author: Duncan
"""
import numpy as np
import ATXxmlparse
import sklearn.cluster as c
import json
import pandas as pd

#classpath = ":".join(pyspark.classpath_jars())
def create_spark():
    spark = SparkSession.builder.getOrCreate()
    return spark
def getTripData(path):
    tripData = ATXxmlparse.getTrips(path)
    for t in tripData:
        (t[1], t[2]) = coordToNetwork((t[1],t[2]),nodes)
        (t[3], t[4]) = coordToNetwork((t[3],t[4]),nodes)
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

def createClusterDict(clusterData, trip_data, nodes):
    clusterDict = {}
    for i in range(len(trip_data)):
        clusterDict[(trip_data[i][0],trip_data[i][1])] = clusterData[i]
    df = pd.DataFrame.from_dict(clusterDict, orient="index")
    df.to_csv("clusterDict.csv")
    #with open("clusterDict.json", "w") as outfile:  
        #json.dump(clusterDict, outfile)
    #for n in nodes.keys():
    #for c in ClusterDict.keys():
    #    if nodes[n].get_coords_tup == c:
    #        print('match', nodes[n].get_coords_tup(), c)
    #        nodes[n].cluster = clusterDict[c]
    return clusterDict
#(nodes,edges) = ATXxmlparse.getNetworkTopo('')
def coordToNetwork(coord,nodes):
    minVal = 999
    for n in nodes.keys():
        nCoord = nodes[n].get_coords_tup()
        distance = np.sqrt((nCoord[0]-coord[0])**2 + (nCoord[1] - coord[1])**2)
        if distance < minVal:
            minVal = distance
            minNode = n
    return nodes[minNode].get_coords_tup()
    #for pudo in latlon: