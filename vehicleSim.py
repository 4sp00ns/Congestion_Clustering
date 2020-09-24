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
import datetime as dt

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
        self.arrival_time = 0
        self.route = []
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
    def get_oPUDO(self):
        return self.oPUDO
    def get_dPUDO(self):
        return self.dPUDO
    def get_route(self):
        return self.route
    def __str__(self):
        return self.origin + ',' + self.destination
    def __repr__(self):
        return self.__str__()
    
class PUDO(object):
    def __init__(self, node, cluster, capacity):
        self.node = node
        self.cluster = cluster
        self.capacity = capacity
    def get_node(self):
        return self.node
    def get_cluster(self):
        return self.cluster
    def get_capacity(self):
        return self.capacity
        
class Node(object):
    def __init__(self, ID, lat, long, cluster):
        self.ID = ID
        self.lat = lat
        self.long = long
        self.cluster = cluster
        self.PUDO = 0
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
    ####this is a list of lists, each sublist is [time,o(lat,long),d(lat,long)]
    tripList = pd.read_csv('nodedTripList.csv').values.tolist()
    clusterList = pd.read_csv('clusterDict.csv').values.tolist()
    
    PUDOList = pd.read_csv('network_PUDOs.csv').values.tolist()
    global nodeDict, edgeDict
    (nodeDict,edgeDict) = ATXxmlparse.getNetworkTopo(config['networkXML'])
    for c in clusterList:
        nodeDict[c[0]].cluster = c[1]
    return (tripList, PUDOList) #(trips, PUDOList, nodeList, edgeList, clusterDict)

def createDataStructures(PUDOList,tripList):
    createNetwork()
    nodeDict['3640042034'].cluster = 43
    nodeDict['151437669'].cluster = 43
    global adjDict, PUDOs
    adjDict = getClusterAdjacencies()
    PUDOs = buildPUDOs(PUDOList)
    schedule = createSchedule(tripList)
    return PUDOs, schedule
    
def createNetwork():
    global ATXnet
    ATXnet = nx.Graph()
    ATXnet.add_nodes_from(nodeDict.keys())
    for e in edgeDict.keys():
        ATXnet.add_edge(e[0],e[1],weight=float(e[2]))
    #return ATXnet
def buildPUDOs(PUDOList):
    PUDOs = {}
    for p in PUDOList:
        station = PUDO(nodeDict[p[1]], nodeDict[p[1]].get_cluster(),5)
        nodeDict[p[1]].PUDO = station
        PUDOs[p[1]] = station
        #PUDOs.append(PUDO(p[o], clusterDict[p[0]],[]))
    return PUDOs

def generateVehicles(vehicleCount, PUDOs):
    for numV in range(int(config['numvehicles'])):
        PUDOs
        numpy.random.choice(PUDOs)
    return PUDOs
        
def createSchedule(tripList):
    #    def __init__(self, ID, hail_time, origin, destination, oPUDO, dPUDO):
    schedule={}
    for t in tripList[:config['numtrips']]:
        eTime = dt.datetime.strptime(t[1], '%H:%M:%S')
        schedule[eTime] = Event(eTime, \
                                'Ride',\
                                Ride(t[0],\
                                     eTime,\
                                     t[2],\
                                     t[3],\
                                     findPUDO(nodeDict[t[2]]),\
                                     findPUDO(nodeDict[t[3]])))
    return schedule

def getClusterAdjacencies():
    clusterList = []
    global adjDict
    adjDict = {}
    for e in edgeDict.keys():
        #print(e)
        clusterList.append((nodeDict[e[0]].get_cluster(),nodeDict[e[1]].get_cluster()))
    uniqAdj = list(set(clusterList))
    for u in uniqAdj:
        adjDict[u[0]] = []
    for u in uniqAdj:
        adjDict[u[0]].append(u[1])
    return adjDict


 
##############################################
######SIMULATION OPERATION FUNCTIONS##########
##############################################

#def findCluster(clusterDict, node):
#    return clusterDict[node]
def findPUDO(node):
    nearbyPUDOs = []
    #cluster = findCluster(clusterDict, node)
    adjClusters = adjDict[node.get_cluster()]
    for p in PUDOs.keys():
        if PUDOs[p].get_cluster() in adjClusters:
            nearbyPUDOs.append(p)
    minDist = 99999
    minPUDO = nearbyPUDOs[0]
    for n in nearbyPUDOs:
        dist = shortestPath(node.get_ID(), n)[1]
        if dist < minDist:
            minPUDO = PUDOs[n]
            minDist = dist
    return minPUDO

def shortestPath(origin, destination):
    path = nx.astar_path(ATXnet,origin,destination,weight='weight')
    distance = nx.astar_path_length(ATXnet,origin,destination, weight='weight')
    return (path, distance)

def assignVehicle(ride): #,enrouteDict):
    oPUDO = ride.get_oPUDO()
    if oPUDO.get_capacity()>0:
        print('reducing PUDO capacity by 1 at' + oPUDO.get_node().get_ID())
        oPUDO.capacity = oPUDO.capacity-1
    #check if origin PUDO has a vehicle, if so, assign
    #if not, check enroute vehicles for rideshare
    #if none, find available vehicle at adjacent pudo and relocate it (is this is a new event?)
    #add the trip keyed by a tuple of origin and destination times to the enrouteDict
    else:
        print(oPUDO.get_node().get_ID())
        enrouteCheck(ride, enrouteDict) 

def enrouteCheck(ride, enrouteDict):
    oCluster = ride.get_origin().get_cluster()
    dCluster = ride.get_destination().get_cluster()
    for route in enrouteDict.keys():
        elapsed = route - ride.get_hail_time()
        adjclusters = []
        timesum = 0
        ####this doesnt work because we dont have travel time, we have length
        for pos in range(len(enrouteDict[route])-1):
            timesum+= edgeDict[(enrouteDict[route][pos],enrouteDict[route][pos+1])]
            adjClusters.append(adjDict[ride.get_origin().get_cluster()])
        if oCluster in adjClusters and dCluster in adjClusters:
            #time - key value = elapsed time
            #step
    #when a vehicle takes a ride, it posts a routelist
    #check routelists for the nodes within the cluster
        #for trips passing through the cluster, check if destination is within the clusters on the route
        #if origin and destination are within route clusters, A* the vehicle to the origin PUDO for pickup
            #delete from routelist
            #use original departure time to calculate arrival time
            #closer destination is first dropoff, may need to combine 2 A* here
            pass
    return vnode            

def findVehicleEnroute(trip, time):
    #calculate current time - start time
    #walk along the link list in the trip summing travel times until you exceed the time passed
    pass
def cleanEnroute(enrouteDict, time):
    #walk through tuple keys in enrouteDict removing entries where the second item 
    #in the tuple is prior to the current time (trip ended)
    #perhaps this could be a stricter assumption to improve computational time
    pass
def masterEventHandler(event, schedule):
    #arrivals
    if event.get_eType() == 'Arrival':
        event.get_eObj.get_destination().get_PUDO().capacity +=1
        enrouteDict.pop(event.get_eObj.get_hail_time())
        pass
        #add vehicle to destination PUDO capacity
        #log data from ride
    #relocates?
    #ride requests
    if event.get_eType() == 'Ride':
        ride = event.get_eObj()
        path = shortestPath(ride.get_origin(), ride.get_destination())
        ride.route = path[0]
        assignVehicle(ride)#,enrouteList)
        #enrouteDict[ride.get_hail_time()] = ride.get_route()
        ride.arrival_time = ride.get_hail_time()+dt.timedelta(seconds = path[1])
        schedule[ride.get_arrival_time()] = Event(ride.get_arrival_time(), 'arrival',ride)
        #event.get_eObj().arrival_time = currTime + travel_time
        #schedule[currTime+travel_time] = Event(currTime + travel_time, 'arrival',ride)
        #add vehicle to enroute
    return schedule   
def getNextEvent(schedule):
    time = min(schedule.keys())
    #delta_t = currTime - oldTime
    event = schedule.pop(time)
    return event#, delta_t
def addEvent(time, eventType, schedule):
    schedule

def simMaster():
    global config
    config = getConfig()
    (tripList,PUDOList) = readData()
    PUDOs, schedule = createDataStructures(PUDOList, tripList)
    while len(schedule) > 0:
        event = getNextEvent(schedule)
        print(event.get_eTime(), event.get_eType())
        schedule = masterEventHandler(event, schedule)
    
    
    pass

#config = getConfig()
#(tripList,nodeDict,edgeDict,PUDOlist,clusterDict) = readData() 
#network = createNetwork(nodeDict,edgeDict)
#test = shortestPath(network, n[0],n[1])