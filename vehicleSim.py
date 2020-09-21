# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 22:26:52 2020

@author: Duncan.Anderson
"""

import networkx as nx
import ATXxmlparse
#import pudo_work
import os
import json
import pandas as pd
from datetime import datetime

#class Vehicle(object):
#    def __init__(self, ID, rider, position, route, objective, charge_level, seats, bid_price, energy_rate, actioncount):
#        self.ID = ID
#        self.rider = rider
#        self.position = position
#        self.route= []
#        self.objective = 'vacant'
#        self.charge_level = charge_level
#        self.seats=seats
#
#    def get_ID(self):
#        return self.ID
#    def get_position(self):
#        return self.position
#    def get_route(self):
#        return self.route
#    def get_charge_level(self):
#        return self.charge_level
#    def get_rider(self):
#        return self.rider
#    def __str__(self):
#        return 'vehicle ' + str(self.ID) + ' at ' + str(self.position) + ' node, currently: ' + str(self.objective)
#    def __repr__(self):
#        return self.__str__()

        
class Ride(object):
    def __init__(self, ID, hail_time, origin, destination, oPUDO, dPUDO):
        self.ID = ID
        self.hail_time = hail_time
        self.origin = origin
        self.destination = destination
        self.oPUDO  = oPUDO
        self.dPUDO = dPUDO
        self.arrival_time = hail_time
    def get_ID(self):
        return self.ID
    def get_hail_time(self):
        return self.hail_time
    def get_arrival_time(self):
        return self.arrival_time
    def get_origin(self):
        return self.origin
    def get_destination(self):
        return self.destination
    def __str__(self):
        return self.origin + ',' + self.destination
    def __repr__(self):
        return self.__str__()
    
class PUDO(object):
    def __init__(self, node, cluster, capacity):
        self.node = node
        self.cluster = cluster
        self.capacity = capacity
        
class Node(object):
    def __init__(self, ID, lat, long, cluster):
        self.ID = ID
        self.lat = lat
        self.long = long
        self.cluster = cluster
    def get_ID(self):
        return self.ID
    def get_lat(self):
        return self.lat
    def get_long(self):
        return self.long
    def get_cluster(self):
        return self.cluster
    def get_PUDO(self):
        return self.PUDO
    def get_coords_tup(self):
        return (self.lat,self.long)
    def get_coords_str(self):
        return (str(self.lat)+','+str(self.long))
    
class Edge(object):
    def __init__(self, ID, head, tail, distance, travel_time):
        self.ID = ID
        self.head = head
        self.tail = tail
        self.distance = distance
        self.travel_time = travel_time
    def get_head(self):
        return self.lat
    def get_tail(self):
        return self.long
    def get_distance(self):
        return self.cluster
    def get_travel_time(self):
        return self.travel_time

class Event(object):
    def __init__(self,eTime,eType,eObj):
        self.eTime = eTime
        self.eType = eType
        self.eObj = eObj
    def get_eTime(self):
        return self.eTime
    def get_eType(self):
        return self.eType
    def get_eObj(self):
        return self.eObj


###LOAD THE LIST OF TRIPS OR GENERATE IT
    #TURN IT INTO A SCHEDULE

####lOAD THE LIST OF PUDO CLUSTERS
    ###CAN WE PREDEFINE ADJACENT CLUSTERS?

###GENERATE THE VEHICLES (STORED IN PUDOS CAPACITY)
    #HOW TO CHOOSE THEIR PLACEMENT

###GENERATE THE NETWORK

###STEP THROUGH THE SCHEDULE POPPING THE MINIMUM EVENT
    #CHARGE VEHICLES AT PUDOS
    
    #FOR TRIP EVENTS:
        #FIND NEAREST PUDO TO ORIGIN + NEAREST PUDO TO DESTINATION
            #CALC WALKING DISTANCES
            #CALC WALKING TIMES
            #ASSIGN A VEHICLE -- ONE ALREADY AT ORIGIN PUDO OR NEARBY PUDO IF NONE ARE AVAILABLE
                #CHECK FOR ENROUTE VEHICLES WITH CURRENT LOCATION AND DESTINATION PUDOS THAT MEET A DISTANCE CRITERIA
                    #CURRENT LOCATION MUST BE X DISTANCE FROM ORIGIN PUDO
                    #DESTINATION PUDO MUST BE Y DIRTANCE FROM CURRENT DESTINATION PUDO
                        #MAYBE A VERY LOW THRESHOLD BUT CHECK EVERY NODE IN THE VEHICLES ROUTE?
            #SEND VEHICLE TO PUDO
        #TTL TRAVEL TIME = MAX(WALK TIME, VEHICLE->PUDO TIME) + TRAVEL TIME TO DEST PUDO + WALK TIME FROM DEST PUDO TO DEST
        
        #NOT ALL EVENTS ARE DELIVERIES?
        #ASSIGN A vehicle TO THE DELIVERY
        #CALCULATE ARRIVAL TIME
            #CALCULATE TRAVEL TIME
            #ADD PUDO ARRIVAL AS AN EVENT
    #FOR ARRIVAL EVENTS
        #ADJUST VEHICLE CAPACITY BY 1
        #LOG DATA
            #TOTAL DISTANCE WALKED
            #TOTAL TRAVEL TIME
            #TOTAL RIDESHARE DISTANCE
            #TOTAL DISTANCE
            
#############################################
##########INITIALIZATION FUNCTIONS###########
#############################################
def getConfig():
    cwd = os.getcwd()
    with open(cwd+r'\config.json') as f:
        config = json.load(f)            
    return config        
def readData():
    #load trip data
    #load pudo locations
    #load clusters
    #load network nodes and edges
    clusterDict = {}
    ####this is a list of lists, each sublist is [time,o(lat,long),d(lat,long)]
    tripList = pd.read_csv('nodedTripList.csv').values.tolist()
    clusterList = pd.read_csv('clusterDict.csv').values.tolist()
    for c in clusterList:
        clusterDict[c[0]] = c[1]
    PUDOList = pd.read_csv('network_PUDOs.csv').values.tolist()
    (nodeDict,edgeDict) = ATXxmlparse.getNetworkTopo(config['networkXML'])
    return (tripList, nodeDict, edgeDict,PUDOList,clusterDict) #(trips, PUDOList, nodeList, edgeList, clusterDict)
def createDataStructures():
    ATXNetwork = createNetwork(nodeDict,edgeDict)
    clusterDict['3640042034'] = 43
    clusterDict['151437669'] = 43
    adjDict = getClusterAdjacencies(edgeDict, clusterDict)
    PUDOs, nodeDict = buildPUDOs(PUDOList, clusterDict, nodeDict)
    
def createNetwork(nodeDict, edgeDict):
    Network = nx.Graph()
    Network.add_nodes_from(nodeDict.keys())
    for e in edgeDict.keys():
        Network.add_edge(e[0],e[1],weight=float(e[2]))
    return Network
def buildPUDOs(PUDOList, clusterDict, nodeDict):
    PUDOs = {}
    for p in PUDOList:
        nodeDict[p[1]].PUDO = PUDO(p[1], clusterDict[p[0]],[])
        PUDOs[p[1]] = nodeDict[p[1]]
        #PUDOs.append(PUDO(p[o], clusterDict[p[0]],[]))
    return PUDOs, nodeDict

def generateVehicles(vehicleCount, PUDOlist):
    for numV in range(int(config['numvehicles'])):
        pass
    randct = np.random.randint(0, len(PUDOlist))
    PUDOlist[randct]
    return vehicleList, PUDOlist
        
def createSchedule(tripList):
    #    def __init__(self, ID, hail_time, origin, destination, oPUDO, dPUDO):
    schedule={}
    for t in tripList[:config['numtrips']]:
        eTime = datetime.strptime(t[1], '%H:%M:%S')
        schedule[eTime] = Event(eTime, 'Ride', Ride(t[0],t[1],t[2],t[3],findPUDO(t[2], PUDOList),findPUDO(t[3], PUDOList)))
    return schedule

def getClusterAdjacencies(edgeDict,clusterDict):
    clusterList = []
    adjDict = {}
    for e in edgeDict.keys():
        #print(e)
        clusterList.append((clusterDict[e[0]],clusterDict[e[1]]))
    uniqAdj = list(set(clusterList))
    for u in uniqAdj:
        adjDict[u[0]] = []
    for u in uniqAdj:
        adjDict[u[0]].append(u[1])
    return adjDict


 
##############################################
######SIMULATION OPERATION FUNCTIONS##########
##############################################

def findCluster(clusterDict, node):
    return clusterDict[node]
def findPUDO(node, PUDOList):
    nearbyPUDOs = []
    cluster = findCluster(clusterDict, node)
    adjClusters = adjDict[cluster]
    for p in PUDOList:
        if clusterDict[p[1]] in adjClusters:
            nearbyPUDOs.append(p)
    minDist = 999
    minPUDO = nearbyPUDOs[0][1]
    for n in nearbyPUDOs:
        dist = shortestPath(ATXNetwork,node, n[1])[1]
        if dist < minDist:
            minPUDO = n[1]
            minDist = dist
    return minPUDO

def shortestPath(Network, origin, destination):
    path = nx.astar_path(Network,origin,destination,weight='weight')
    distance = nx.astar_path_length(Network,origin,destination, weight='weight')
    return (path, distance)

def assignVehicle(PUDOs, ride,enrouteDict):
    oPUDO = PUDOs[ride.get_oPUDO()]
    if len(oPUDO.get_capacity())>0:
        vehicle = oPUDO.get_capacity()[0]
        oPUDO.capacity = oPUDO.capacity[1:]
    #check if origin PUDO has a vehicle, if so, assign
    #if not, check enroute vehicles for rideshare
    #if none, find available vehicle at adjacent pudo and relocate it (is this is a new event?)
    #add the trip keyed by a tuple of origin and destination times to the enrouteDict
    else:
        enrouteCheck(ride, enrouteDict, clusterDict)
    return 

def enrouteCheck(ride, enrouteDict,clusterDict):
    #when a vehicle takes a ride, it posts a routelist
    #check routelists for the nodes within the cluster
        #for trips passing through the cluster, check if destination is within the clusters on the route
        #if origin and destination are within route clusters, A* the vehicle to the origin PUDO for pickup
            #delete from routelist
            #use original departure time to calculate arrival time
            #closer destination is first dropoff, may need to combine 2 A* here
    pass            

def findVehicleEnroute(trip, time):
    #calculate current time - start time
    #walk along the link list in the trip summing travel times until you exceed the time passed
    pass
def cleanEnroute(enrouteDict, time):
    #walk through tuple keys in enrouteDict removing entries where the second item 
    #in the tuple is prior to the current time (trip ended)
    #perhaps this could be a stricter assumption to improve computational time
    pass
def masterEventHandler(event):
    #arrivals
    if event.get_eType() == 'arrival':
        pass
        #add vehicle to destination PUDO capacity
        #log data from ride
    #relocates?
    #ride requests
    if event.get_eType() == 'ride':
        event.get_eObj().vehicle = assignVehicle(ride,enrouteList)
        travel_time = 1
        event.get_eObj().arrival_time = currTime + travel_time
        schedule[currTime+travel_time] = Event(currTime + travel_time, 'arrival',ride)
        #add vehicle to enroute
    pass    
def getNextEvent(schedule):
    currTime = min(schedule.keys())
    delta_t = currTime - oldTime
    event = schedule.pop(time)
    return event, delta_t
def addEvent(time, eventType, schedule):
    schedule

def simMaster():
    while len(schedule) > 0:
        event, delta_t = getNextEvent(schedule)
        event = masterEventHandler(event)
    
    
    pass

#config = getConfig()
#(tripList,nodeDict,edgeDict,PUDOlist,clusterDict) = readData() 
#network = createNetwork(nodeDict,edgeDict)
#test = shortestPath(network, n[0],n[1])