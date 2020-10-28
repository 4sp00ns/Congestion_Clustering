# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 12:32:42 2020

@author: Duncan
"""
import numpy as np
import ATXxmlparse
import sklearn.cluster as c
#import json
import pandas as pd
from datetime import datetime

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
    for n in nodes.values():
        #print(n.get_lat(), len(str(n.get_lat())))
        lat = n.get_lat()
        rnd = round(lat,2)
        trunc = float(str(lat)[:5])
        rndDict[rnd] = []
        truncDict[trunc] = []
    for n in nodes.values():
        lat = n.get_lat()
        rnd = round(lat,2)
        trunc = float(str(lat)[:5])
        rndDict[rnd].append(n.get_coords_tup())
        truncDict[trunc].append(n.get_coords_tup())
    return rndDict, truncDict            

def getTripData(path):
    tripData = ATXxmlparse.getTrips(path)
    return tripData


def allOD(tripData):
    allOD = []
    for t in tripData:
        if t[1] != 'p' and t[3] != 'p' and t[0] != '':
            allOD.append([t[1],t[2]])
            allOD.append([t[3],t[4]])
    for n in nodeDict.keys():
        allOD.append(nodeDict[n].get_coords_tup())
    return allOD

def run_kmeans(trip_locations, nclusters):
    data_array = np.asarray(trip_locations)
    #trip_loc.show()
    #data_array = np.array(trip_loc.collect())
    #for i in range (0,100,25):
    maxiter = 300 + 30*50
    k_means_data = c.k_means(data_array, n_clusters = nclusters, verbose = False)
    centroid_arr = k_means_data[0]
    #txtout = np.asarray(latlon)
    #np.savetxt('testout_weight.csv', txtout, delimiter=',')
    return k_means_data

def kmean_weighted(trip_weighted_node_coordinate_dict, nclusters):
    X=np.array(list(trip_weighted_node_coordinate_dict.keys()))
    weight=np.array(list(trip_weighted_node_coordinate_dict.values()))
    kmeans = c.KMeans(n_clusters=nclusters, random_state=0, verbose = False).fit_predict(X,None,sample_weight=weight)
    return kmeans

def trip_weighted_node_coord(tripList):
    outdict = {}
    for n in nodeDict.values():
        outdict[(n.get_lat(),n.get_long())] = 1
    for t in tripList:
        olat = nodeDict[str(t[1])].get_lat()
        olong = nodeDict[str(t[1])].get_long()
        dlat = nodeDict[str(t[2])].get_lat()
        dlong = nodeDict[str(t[2])].get_long()
        outdict[(olat,olong)] +=1
        outdict[(dlat,dlong)] +=1
    return outdict



def load_gtraffic(edgeDict):
    traffic = pd.read_csv('googlemaps_data_full.csv').values.tolist()
    for t in traffic:
        edgeDict[(str(t[1]),str(t[0]))].duration = t[5]
        edgeDict[(str(t[1]),str(t[0]))].congested_duration = t[6]

def createClusterDict(clusterarr, kmean_input, nodes,outstr,nclusters):
    print('assigning clusters to nodes')
    clusterDict = {}
    cDict = {}
    #for n in nodes.keys():
        #cdict matches coordinates to node_IDs
        #cDict[nodes[n].get_coords_tup()] = nodes[n].get_ID()
    for n in nodes.values():
        #print(i)
        #clusterDict[(allOD[i][0],allOD[i][1])] = clusterData[i]
        pos = kmean_input.index(n.get_coords_tup())
        clusterDict[n.get_ID()] = [n.get_lat(),n.get_long(),clusterarr[pos]]
        #clusterDict[cDict[(allOD[i][0],allOD[i][1])]] = clusterData[i]
    pd.DataFrame.from_dict(clusterDict, orient="index", columns = [\
                                          'lat'\
                                          ,'long'\
                                          ,'cluster'])\
                        .to_csv("CLUSTERS\\clusterDict_"+outstr+str(nclusters)+".csv")
    #with open("clusterDict.json", "w") as outfile:  
        #json.dump(clusterDict, outfile)
    #for n in nodes.keys():
    #for c in ClusterDict.keys():
    #    if nodes[n].get_coords_tup == c:
    #        print('match', nodes[n].get_coords_tup(), c)
    #        nodes[n].cluster = clusterDict[c]
    return clusterDict
#(nodes,edges) = ATXxmlparse.getNetworkTopo('')
def PUDOtoNetwork(centroids,nodes, outstr, nclusters):
    print('mapping PUDOs (cluster centroids) to network nodes')
    outlist = []
    for c in centroids:
        node = bruteCoord((c[0],c[1]),nodes)
        #print(node)
        outlist.append([nodes[node].get_ID(), nodes[node].get_lat(), nodes[node].get_long()])
    pd.DataFrame(outlist, columns = ['id','lat','long']).to_csv("PUDOS\\network_PUDOs"+outstr+str(nclusters)+".csv",index = False)
    return outlist
def coordToNetwork(coord,truncDict, rndDict):
    #matches a coordinate to the indexed lists
    #if fails, calls brutecoord
    minVal = 99999
    #print(coord)
    rnd = round(coord[0],2)
    flt = float(str(coord[0])[:5])
    try:
        nodelist = truncDict[flt] + rndDict[rnd]
    except:
        print('coord error, calling brute force')
        minNode = nodeDict[bruteCoord(coord,nodeDict)].get_coords_tup()
        return minNode
    for n in nodelist:
        #nCoord = nodes[n].get_coords_tup()
        distance = np.sqrt((n[0]-coord[0])**2 + (n[1] - coord[1])**2)
        if distance < minVal:
            minVal = distance
            minNode = n            
    if minVal > .2:
        print('distance too high, calling brute force')
        minNode = nodeDict[bruteCoord(coord,nodeDict)].get_coords_tup()
    return minNode

def bruteCoord(coord, nodes):
    #brute force matching method,called when index method fails
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
    #converts coordinate trips (o lat/long -> d lat/long)
    #to node IDed trips
    #trips must first be matched to node coordinates
    print('matching trips to node IDs')
    nodedTrips = []
    cDict = {}
    for n in nodeDict.keys():
        cDict[nodeDict[n].get_coords_tup()] = nodeDict[n].get_ID()
    pd.DataFrame.from_dict(cDict, orient='index').to_csv('cdict.csv')
    for t in tdata:
        ocoord = (round(t[6],6),round(t[7],6))
        dcoord = (round(t[8],6),round(t[9],6))
        nodedTrips.append([t[1],cDict[ocoord],cDict[dcoord]])
        #print(len(nodedTrips))
    pd.DataFrame(nodedTrips, columns = ['time','origin','destination'])\
                .to_csv('nodedTripList.csv', index = False)
    return nodedTrips

def coordinateMatchTrips(trips):
    #matches trips to coordinates of nodes in the network
    trip_coord_map = []
    try:
        
        trip_coord_map = pd.read_csv('coordTrips.csv').values.tolist()
        print('loading existing coordinate matched trips')
    except:
        print('existing coordinate matched trips not found, regenerating')
        x=0
        for t in trips:
            #print(t)                    
            x+=1
            if x%1000 == 0:
                print(x)
            new_o = coordToNetwork((t[1],t[2]),truncDict, rndDict)
            new_d = coordToNetwork((t[3],t[4]),truncDict, rndDict)
            trip_coord_map.append([t[0],t[1],t[2],t[3],t[4]\
                                   ,new_o[0], new_o[1], new_d[0],new_d[1]])
        pd.DataFrame(trip_coord_map).to_csv('coordTrips.csv', index = False)
    return trip_coord_map

def calc_centroid(clusterDict,nclusters):
    outlist = []
    for clus in range(nclusters):
        sumlat = 0
        sumlong = 0
        ct = 0
        #print('DEBUG cluster',clus)
        for n in clusterDict.keys():
            #print('DEBUG',clusterDict[n][2], clus)
            if clusterDict[n][2] == clus:
                ct+=1
                sumlat += nodeDict[str(n)].get_lat()
                sumlong += nodeDict[str(n)].get_long()
        outlist.append((sumlat/ct,sumlong/ct))
    return outlist

def load_all():
    (nodeDict,edgeDict) = ATXxmlparse.getSDBNetworkTopo()
    tripData = getTripData('')
    #rndDict, truncDict = dictIndexCoords(nodeDict)
    #coordTrips = coordinateMatchTrips(tripData)
    #nodedTripList = nodedTripList(coordTrips)
    ALLOD = allOD(tripData)
    trip_weight_nodes = trip_weighted_node_coord(tripList)
    return ALLOD, trip_weight_nodes, nodeDict, edgeDict
def gen_communities():
    ALLOD, trip_weight_nodes, nodeDict, edgeDict = load_all()
    trip_weight_nodes = prune_nodedict_to_urban_core(trip_weight_nodes)
    for nclusters in range(20,100,20):
        print('clustercount',nclusters, datetime.now())
        #print('running kmeans1', datetime.now())
        #kmd = run_kmeans(ALLOD, nclusters)
        print('running kmeans weighted', datetime.now())
        kmw = kmean_weighted(trip_weight_nodes,nclusters)
        print('creating clusterdicts', datetime.now())
        #clusterDict = createClusterDict(kmd[1],ALLOD, nodeDict,'kmean_rawtrips', nclusters)
        clusterDict_w = createClusterDict(kmw,list(trip_weight_nodes.keys()), nodeDict, 'kmean_uc',nclusters)        
        centroids_w = calc_centroid(clusterDict_w, nclusters)
        print('creating pudolists')
        #PUDOlist= PUDOtoNetwork(kmd[0],nodeDict,'kmean_rawtrips',nclusters)
        
        ####PUDO SELECTION SHOULD INVOLVE THE RANGE METRIC
        #PUDOlist_w = PUDOtoNetwork(centroids_w, nodeDict,'kmean_uc',nclusters)
        
#####will just handle this in the sim I guess?
#def prune_nodedict_to_urban_core(any_dict):
#    uc_list = pd.read_csv('urban_core_nodes.csv').values.tolist()
#    newdict = {}
#    for uc in uc_list:
#        newdict[(uc[2],uc[3])] = 0
#    for coord in any_dict.keys():
#        if coord in newdict.keys():
#            newdict[coord] = any_dict[coord]
#    return newdict 