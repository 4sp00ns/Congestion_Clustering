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
    def get_ID(self):
        return self.get_node().get_ID()
        
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
    def __init__(self, ID, tail, head, capacity, length, fft, b, exponent, speed):
        self.ID = ID
        self.head = head
        self.tail = tail
        self.capacity = capacity
        self.length = length
        self.fft = fft
        self.b = b
        self.exponent = exponent
        self.speed = speed
        self.congested_speed = fft
    def get_head(self):
        return self.head
    def get_tail(self):
        return self.tail
    def get_capacity(self):
        return self.capacity
    def get_length(self):
        return self.length
    def get_fft(self):
        return self.fft
    def get_b(self):
        return self.b
    def get_exponent(self):
        return self.exponent
    def get_speed(self):
        return self.speed
    def get_congested_speed(self):
        return self.congested_speed

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
    try:
        nodeDict, edgeDict
    except:
        print('reloading nodeDict and edgeDict')
        (nodeDict,edgeDict) = ATXxmlparse.getSDBNetworkTopo()
    for c in clusterList:
        nodeDict[str(c[0])].cluster = c[1]
    return (tripList, PUDOList) #(trips, PUDOList, nodeList, edgeList, clusterDict)

def createDataStructures(PUDOList,tripList):
    createNetwork()
    #nodeDict['3640042034'].cluster = 43
    #nodeDict['151437669'].cluster = 43
    global adjDict, PUDOs, enrouteDict
    adjDict = getClusterAdjacencies()
    PUDOs = buildPUDOs(PUDOList)
    schedule = createSchedule(tripList)
    enrouteDict = {}
    return PUDOs, schedule
    
def createNetwork():
    print('building network')
    global ATXnet
    ATXnet = nx.Graph()
    ATXnet.add_nodes_from(nodeDict.keys())
    for e in edgeDict.keys():
        ATXnet.add_edge(e[0],e[1],weight=float(edgeDict[e].get_fft()))
    #return ATXnet
def buildPUDOs(PUDOList):
    print('building PUDOs')
    PUDOs = {}
    for p in PUDOList:
        station = PUDO(nodeDict[str(p[0])], nodeDict[str(p[0])].get_cluster(),1)
        nodeDict[str(p[0])].PUDO = station
        PUDOs[str(p[0])] = station
        #PUDOs.append(PUDO(p[o], clusterDict[p[0]],[]))
        
        ####TEMP FOR DEBUGGING RIDESHARE####
    nodeDict['2281'].PUDO.capacity = 0
    return PUDOs

def generateVehicles(vehicleCount, PUDOs):
    print('placing vehicles based on config')
    for numV in range(int(config['numvehicles'])):
        PUDOs
        numpy.random.choice(PUDOs)
    return PUDOs
        
def createSchedule(tripList):
    print('building initial schedule')
    #    def __init__(self, ID, hail_time, origin, destination, oPUDO, dPUDO):
    schedule={}
    for t in tripList[:config['numtrips']]:
        print('schedule',t)
        eTime = dt.datetime.strptime(t[0], '%H:%M:%S')
        schedule[eTime] = Event(eTime, \
                                'Ride',\
                                Ride(t[0],\
                                     eTime,\
                                     str(t[1]),\
                                     str(t[2]),\
                                     findPUDO(nodeDict[str(t[1])])[0],\
                                     findPUDO(nodeDict[str(t[2])])[0]))
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
    #ranks adjacent PUDOS by proximity to the node and returns an ordered list
    nearbyPUDOs = {}
    #cluster = findCluster(clusterDict, node)
    adjClusters = adjDict[node.get_cluster()]
    for p in PUDOs.values():
        if p.get_cluster() in adjClusters:
            nearbyPUDOs[p] = shortestPath(node.get_ID(), p.get_ID())[1]
    outlist = sorted(nearbyPUDOs, key=nearbyPUDOs.get)
    
#    minDist = 99999
#    minPUDO = nearbyPUDOs[0]
#    for n in nearbyPUDOs:
#        dist = shortestPath(node.get_ID(), n.get_ID())[1]
#        if dist < minDist:
#            minPUDO = n
#            minDist = dist
    return outlist

def shortestPath(origin, destination):
    path = nx.astar_path(ATXnet,str(origin),str(destination),weight='weight')
    distance = nx.astar_path_length(ATXnet,str(origin),str(destination), weight='weight')
    return (path, distance)

def assignVehicle(ride, schedule): #,enrouteDict):
    oPUDO = ride.get_oPUDO()
    if oPUDO.get_capacity()>0:
        #if the default origin PUDO has a vehicle it is assigned to the ride
        oPUDO.capacity -= 1
        enrouteDict[ride.get_hail_time()] = ride
        schedule[ride.get_arrival_time()] = Event(ride.get_arrival_time(), 'Arrival',ride)
    else:
        #if not, we check ongoing rides for compatible rideshares
        print('no vehicle available, checking rideshare')
        sharing, schedule = rideshareLogic(ride, enrouteDict, schedule)
        if sharing == False:
            print('no rideshare, pulling from adjacent PUDOs')
            #if no compatible rideshares are found we pull the PUDOs of the adjacent clusters
            nearbyPUDOs = findPUDO(nodeDict[ride.get_origin()])
            #walk through adjacent PUDOs (in order of distance) and find nearest PUDO with capacity
            for np in nearbyPUDOs:
                if np.get_capacity() > 0:
                    chosenPUDO = np
                    break
            #strip a vehicle from that PUDO and assign it
            chosenPUDO.capacity -= 1
            #recalculate the route and arrival time to include the vehicle travel
            #from the adjacent PUDO to the origin PUDO
            rideExt = shortestPath(chosenPUDO.get_ID(), ride.get_oPUDO().get_ID())
            ride.route = rideExt[0] + ride.get_route()
            ride.arrival_time = ride.get_arrival_time() + dt.timedelta(minutes = rideExt[1])
            #add the extended route to enroute trips and add the arrival to the schedule
            enrouteDict[ride.get_hail_time()] = ride
            schedule[ride.get_arrival_time()] = Event(ride.get_arrival_time(), 'Arrival',ride)            
                    
                    
            
    return schedule

def rideshareLogic(ride, enrouteDict, schedule):
    oCluster = nodeDict[str(ride.get_origin())].get_cluster()
    dCluster = nodeDict[str(ride.get_destination())].get_cluster()
    rideClusters = []
    for nod in ride.get_route():
        rideClusters += adjDict[nodeDict[nod].get_cluster()]
    for rkey in enrouteDict.keys():
        route = enrouteDict[rkey].get_route()
        elapsed = ride.get_hail_time() - rkey 
        adjClusters = []
        timesum = dt.timedelta()
        reduce1 = {}
        #print('ROUTE DEBUG',route)
        vehicle_loc = route[0]
        for pos in range(len(route)-1):
            timesum += dt.timedelta(minutes=edgeDict[(route[pos],route[pos+1])].get_fft())
            if timesum < elapsed:
                vehicle_loc = route[pos]
            if timesum > elapsed:
                adjClusters += adjDict[nodeDict[str(route[pos])].get_cluster()]
        if oCluster in adjClusters:
            if dCluster in adjClusters or nodeDict[route[-1:][0]].get_cluster() in rideClusters:
                print('found a compatible route')
                #remove existing arrival
                schedule.pop(enrouteDict[rkey].get_arrival_time())
                #calculate route diversion
                rideDiversion = shortestPath(vehicle_loc, ride.get_origin())
                #calculate new route and route extension, depending on whom is dropped off first
                if dCluster in adjClusters:
                    #if the current route contains both origin and destination
                    #the rideshare is dropped first
                    newRoute = shortestPath(ride.get_oPUDO().get_ID(), ride.get_dPUDO().get_ID())
                    rideExt = shortestPath(ride.get_dPUDO().get_ID(), route[-1:][0])
                    objA = ride
                    objB = enrouteDict[rkey]
                    ride.arrival_time = ride.get_hail_time()+dt.timedelta(minutes = rideDiversion[1]+newRoute[1])
                    enrouteDict[rkey].arrival_time = ride.get_arrival_time() + dt.timedelta(minutes = rideExt[1])
                if nodeDict[route[-1:][0]].get_cluster() in rideClusters:
                    #if the rideshare requires extending the current route
                    #the current rider is dropped first
                    newRoute = shortestPath(ride.get_oPUDO().get_ID(), route[-1:][0])
                    rideExt = shortestPath(route[-1:][0], ride.get_dPUDO().get_ID())
                    objA = enrouteDict[rkey]
                    objB = ride
                    enrouteDict[rkey].arrival_time = ride.get_hail_time()+dt.timedelta(minutes = rideDiversion[1]+newRoute[1])
                    ride.arrival_time = enrouteDict[rkey].arrival_time + dt.timedelta(minutes = rideExt[1])
                ###there may be a cleaner way to do this
                firstArrival = ride.get_hail_time()+dt.timedelta(minutes = rideDiversion[1]+newRoute[1])
                finalArrival = firstArrival + dt.timedelta(minutes = rideExt[1])
                schedule[firstArrival] = Event(firstArrival,'Rideshare',objA)
                schedule[finalArrival] = Event(finalArrival, 'Arrival', objB)
                return True, schedule
    return False, schedule   

def cleanEnroute(enrouteDict, time):
    #walk through tuple keys in enrouteDict removing entries where the second item 
    #in the tuple is prior to the current time (trip ended)
    #perhaps this could be a stricter assumption to improve computational time
    pass
def masterEventHandler(event, schedule):
    #arrivals
    if event.get_eType() == 'Rideshare':
        #print('handling it')
        pass
        #run reporting for the event here
    elif event.get_eType() == 'Arrival':
        #print('DEBUG EOBJ', event.get_eObj())
        nodeDict[str(event.get_eObj().get_dPUDO().get_ID())].PUDO.capacity +=1
        enrouteDict.pop(event.get_eObj().get_hail_time())
        pass
        #add vehicle to destination PUDO capacity
        #log data from ride
    #relocates?
    #ride requests
    elif event.get_eType() == 'Ride':
        ride = event.get_eObj()
        path = shortestPath(ride.get_oPUDO().get_ID(), ride.get_dPUDO().get_ID())
        ride.route = path[0]
        ride.arrival_time = ride.get_hail_time() + dt.timedelta(minutes = path[1])
        schedule = assignVehicle(ride, schedule)#,enrouteList)
        #enrouteDict[ride.get_hail_time()] = ride.get_route()
        ride.arrival_time = ride.get_hail_time()+dt.timedelta(minutes = path[1])
        #event.get_eObj().arrival_time = currTime + travel_time
        #schedule[currTime+travel_time] = Event(currTime + travel_time, 'arrival',ride)
        #add vehicle to enroute
    else:
        print('unhandled event type')
        print(event.get_eType())
    return schedule

def stateReport(verbose):
    #this will first function as a series of debug checks to ensure the model is working correctly
    idleVehicles = 0
    for p in PUDOs.values():
        idleVehicles += p.get_capacity()
    print("# idle vehicles", idleVehicles)
def getNextEvent(schedule):
    time = min(schedule.keys())
    #delta_t = currTime - oldTime
    event = schedule.pop(time)
    return event, schedule#, delta_t

def simMaster():
    global config
    config = getConfig()
    (tripList,PUDOList) = readData()
    PUDOs, schedule = createDataStructures(PUDOList, tripList)
    while len(schedule) > 0:
        event, schedule = getNextEvent(schedule)
        print(event.get_eTime(), event.get_eType())
        schedule = masterEventHandler(event, schedule)
        stateReport(True)
        #state reporting
        
    
    

#config = getConfig()
#(tripList,nodeDict,edgeDict,PUDOlist,clusterDict) = readData() 
#network = createNetwork(nodeDict,edgeDict)
#test = shortestPath(network, n[0],n[1])