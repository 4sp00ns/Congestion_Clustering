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

class Vehicle(object):
    def __init__(self, ID, rider, position, route, objective, charge_level, seats, bid_price, energy_rate, actioncount):
        self.ID = ID
        self.rider = rider
        self.position = position
        self.route= []
        self.objective = 'vacant'
        self.charge_level = charge_level
        self.seats=seats

    def get_ID(self):
        return self.ID
    def get_position(self):
        return self.position
    def get_route(self):
        return self.route
    def get_charge_level(self):
        return self.charge_level
    def get_rider(self):
        return self.rider
    def __str__(self):
        return 'vehicle ' + str(self.ID) + ' at ' + str(self.position) + ' node, currently: ' + str(self.objective)
    def __repr__(self):
        return self.__str__()

        
class Ride(object):
    def __init__(self, ID, hail_time, origin, destination, oPUDO, dPUDO):
        self.origin = origin
        self.destination = destination
        self.hail_time = hail_time
        self.ID = ID
    def get_ID(self):
        return self.ID
    def get_hail_time(self):
        return self.hail_time
    def get_origin(self):
        return self.origin
    def get_destination(self):
        return self.destination
    def __str__(self):
        return str(self.seats) + ' rider at node: ' + str(self.position) + ' travelling to: ' + str(self.destination)
    def __repr__(self):
        return self.__str__()
    
class PUDO(object):
    def __init__(self, node, cluster, capacity):
        self.node = node
        self.cluster = cluster
        self.capacity = capacity
        
class Node(object):
    def __init__(self, ID, lat, long, cluster,PUDO):
        self.ID = ID
        self.lat = lat
        self.long = long
        self.cluster = cluster
        self.PUDO = PUDO
    def get_ID(self):
        return self.ID
    def get_lat(self):
        return self.lat
    def get_long(self):
        return self.long
    def get_cluster(self):
        return self.cluster
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
    pass
    ####this is a list of lists, each sublist is [time,o(lat,long),d(lat,long)]
    tripList = pd.read_csv('nodedTripList.csv').values.tolist()
    clusterDict = pd.read_csv('clusterDict.csv').values.tolist()
    PUDOList = pd.read_csv('network_PUDOs.csv').values.tolist()
    (nodeDict,edgeDict) = ATXxmlparse.getNetworkTopo(config['networkXML'])
    return (tripList, nodeDict, edgeDict,PUDOList,clusterDict) #(trips, PUDOList, nodeList, edgeList, clusterDict)
           
def createNetwork(nodeDict, edgeDict):
    Network = nx.Graph()
    Network.add_nodes_from(nodeDict.keys())
    for e in edgeDict.keys():
        Network.add_edge(e[0],e[1],weight=float(e[2]))
    return Network
def buildPUDO(PUDOList, clusterDict):
    PUDOs = []
    for p in PUDOList:
        PUDOs.append(PUDO(p[o]), clusterDict[p[0]],[])
def createSchedule(tripList):
    #    def __init__(self, ID, hail_time, origin, destination, oPUDO, dPUDO):
    schedule={}
    for t in tripList:
        schedule[t[1]] = Ride(t[0],t[1],t[2],t[3],noneType,noneType)
        
    pass
def clusterAdjacencies(edgeDict,clusterDict):
    clusterList = []
    for e in edgeDict.keys():
        clusterList.append((clusterDict[e[0]],clusterDict[e[1]]))
    toFromSet = set(clusterList)
    return toFromSet
def generateVehicles(vehicleCount, PUDOs):
    for numV in range(int(config['numvehicles'])):
        pass
    
    return vehicleList, PUDOs


def findCluster(clusterDict, position):
    return clusterDict[position]
def findPUDO(position):
    pass

def shortestPath(Network, origin, destination):
    path = nx.astar_path(Network,origin,destination,weight='weight')
    distance = nx.astar_path_length(Network,origin,destination, weight='weight')
    return (path, distance)

def assignVehicle(vehicle,ride,enrouteList):
    #check if origin PUDO has a vehicle, if so, assign
    #if not, check enroute vehicles for rideshare
    #if none, find available vehicle at adjacent pudo and relocate it (is this is a new event?)
    #add the trip keyed by a tuple of origin and destination times to the enrouteDict
    pass

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
    #walk through tuple keys in enrouteDict removing entries where the second item in the tuple is prior to the current time (trip ended)
    #perhaps this could be a stricter assumption to improve computational time
    pass
def masterEventHandler(event):
    #arrivals
    #relocates?
    #ride requests
    #checks the event type and calls the appropriate function to handle
    pass    
def getNextEvent(schedule):
    time = min(schedule.keys())
    event = schedule.pop(time)
    return event
def addEvent(schedule):
    pass

#config = getConfig()
#(tripList,nodeDict,edgeDict,PUDOlist,clusterDict) = readData() 
#network = createNetwork(nodeDict,edgeDict)
#test = shortestPath(network, n[0],n[1])