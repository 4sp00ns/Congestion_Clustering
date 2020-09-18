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


###pull trip data
###match it to nodes in the network (if needed) (hopefully never again)
### convert it into a full list of coords (O+Ds) and add nodes
### push this list through kmeans
###run kmeans[1] thru clusterdict
###convert kmeans[0] to PUDO list (brute force network match)
#classpath = ":".join(pyspark.classpath_jars())

#def master():
#tripData = ATXxmlparse.getTrips('')
#(nodes,edges) = ATXxmlparse.getNetworkTopo('')
#CoordMatchTrips = coordinateMatchTrips(tripData)
#nodedTripList = nodedTripList(CoordMatchTrips)
#allOD = allOD(CoordMatchTrips)
#[centroids, clusters, score] = run_kmeans(allOD)
#clusterDict = createClusterDict(cluster Data,allOD,nodes)
#PUDOlist = PUDOtoNetwork(centroids, nodes)


def dictIndexCoords(nodes):
    rndDict = {}
    truncDict = {}
    for n in nodes.keys():
        lat = nodes[n].get_lat()
        rnd = round(lat,2)
        trunc = float(str(lat)[5:])
        rndDict[rnd] = []
        truncDict[trunc] = []
    for n in nodes.keys():
        for n in nodes.keys():
            lat = nodes[n].get_lat()
            rnd = round(lat,2)
            trunc = float(str(lat)[5:])
            rndDict[rnd] = [n.get_coords_tup()]
            truncDict[trunc] = []
            
        #for n in nodelist:
        #nodedict[round(n[0],3)] = []
        #for n in nodelist:
        #rnd = round(n[0],3)
        #for n in nodelist:
        #rnd = round(n[0],3)
        #nodedict[rnd].append(n)
        #def create_spark():
    spark = SparkSession.builder.getOrCreate()
    return spark
def getTripData(path):
    tripData = ATXxmlparse.getTrips(path)
    return tripData


def allOD(tripData):
    allOD = []
    for t in tripData:
        if t[1] != 'p' and t[3] != 'p' and t[0] != '':
            allOD.append([t[1],t[2]])
            allOD.append([t[3],t[4]])
    for n in nodes.keys():
        allOD.append(nodes[n].get_coords_tup())
    return allOD
def run_kmeans(trip_locations):
    data_array = np.asarray(trip_locations)
    #trip_loc.show()
    #data_array = np.array(trip_loc.collect())
    #for i in range (0,100,25):
    maxiter = 300 + 30*50
    k_means_data = c.k_means(data_array, n_clusters = 500, verbose = True)
    centroid_arr = k_means_data[0]
    #txtout = np.asarray(latlon)
    #np.savetxt('testout_weight.csv', txtout, delimiter=',')
    return k_means_data

def createClusterDict(clusterData, allOD, nodes):
    clusterDict = {}
    for i in range(len(allOD)):
        #print(i)
        clusterDict[(allOD[i][0],allOD[i][1])] = clusterData[i]
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
def PUDOtoNetwork(centroids,nodes):
    outlist = []
    for c in centroids:
        node = bruteCoord((c[0],c[1]),nodes)
        print(node)
        outlist.append([nodes[node].get_ID(), nodes[node].get_coords_tup()])
    return outlist
def coordToNetwork(coord,nodedict, rnddict):
    minVal = 99999
    #print(coord)
    rnd = round(coord[0],2)
    flt = float(str(coord[0])[:5])
    try:
        nodelist = nodedict[flt] + rnddict[rnd]
    except:
        print('coord error, calling brute force')
        minNode = nodes[bruteCoord(coord,nodes)].get_coords_tup()
        return minNode
    for n in nodelist:
        #nCoord = nodes[n].get_coords_tup()
        distance = np.sqrt((n[0]-coord[0])**2 + (n[1] - coord[1])**2)
        if distance < minVal:
            minVal = distance
            minNode = n            
    if minVal > .2:
        print('distance too high, calling brute force')
        minNode = nodes[bruteCoord(coord,nodes)].get_coords_tup()
    return minNode

def bruteCoord(coord, nodes):
    minVal = 99999
    for n in nodes.keys():
        nCoord = nodes[n].get_coords_tup()
        distance = np.sqrt((nCoord[0]-coord[0])**2 + (nCoord[1] - coord[1])**2)
        if distance < minVal:
            minVal = distance
            minNode = n
    return minNode#.get_coords_tup()
    #for pudo in latlon:
def nodedTripList(tdata):
    nodedTrips = []
    cDict = {}
    i=0
    if len(tdata[0]) == 6:
        i=1
    for n in nodes.keys():
        cDict[nodes[n].get_coords_tup()] = nodes[n].get_ID()
    for t in tdata:
        ocoord = (t[1],t[2])
        dcoord = (t[3],t[4])
        nodedTrips.append([t[0],cDict[ocoord],cDict[dcoord]])
#    for t in tdata:
#        for n in nodes.keys():
#            if nodes[n].get_lat() == t[1+i] and nodes[n].get_long() == t[2+i]:
#                print('origin match')
#                for nn in nodes.keys():
#                    if nodes[nn].get_lat() == t[3+i] and nodes[nn].get_long() == t[4+i]:
#                        print('destination match')
#                        nodedTrips.append([t[0], nodes[n].get_ID(), nodes[nn].get_ID()])
                    
    return nodedTrips

def coordinateMatchTrips(trips):
    x=0
    for t in trips:
        #print(t)                    
        x+=1
        if x%1000 == 0:
            print(x)
        (t[1], t[2]) = coordToNetwork((t[1],t[2]),nodedict, rnddict)
        (t[3], t[4]) = coordToNetwork((t[3],t[4]),nodedict, rnddict)
    return trips

#tripData = ATXxmlparse.getTrips('')
#(nodes,edges) = ATXxmlparse.getNetworkTopo('')
#CoordMatchTrips = coordinateMatchTrips(tripData)
#nodedTripList = nodedTripList(CoordMatchTrips)