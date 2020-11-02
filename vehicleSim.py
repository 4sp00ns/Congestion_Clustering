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
import traceback
from pudo_work import load_gtraffic
from ATXxmlparse import Edge
from ATXxmlparse import Node
import numpy as np
        
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
        self.dropped = False
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
    def get_dropped(self):
        return self.dropped
    
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
        return self.node.get_ID()
        
#class Node(object):
#    def __init__(self, ID, lat, long, cluster):
#        self.ID = ID
#        self.lat = lat
#        self.long = long
#        self.cluster = cluster
#        self.PUDO = 0
#    def get_ID(self):
#        return self.ID
#    def get_lat(self):
#        return self.lat
#    def get_long(self):
#        return self.long
#    def get_cluster(self):
#        return self.cluster
#    def get_PUDO(self):
#        return self.PUDO
#    def get_coords_tup(self):
#        return (self.lat,self.long)
#    def get_coords_str(self):
#        return (str(self.lat)+','+str(self.long))
    
#class Edge(object):
#    def __init__(self, ID, tail, head, capacity, length, fft, b, exponent, speed):
#        self.ID = ID
#        self.head = head
#        self.tail = tail
#        self.capacity = capacity
#        self.length = length
#        self.fft = fft
#        self.b = b
#        self.exponent = exponent
#        self.speed = speed
#        self.congested_speed = fft
#    def get_head(self):
#        return self.head
#    def get_tail(self):
#        return self.tail
#    def get_capacity(self):
#        return self.capacity
#    def get_length(self):
#        return self.length
#    def get_fft(self):
#        return self.fft
#    def get_b(self):
#        return self.b
#    def get_exponent(self):
#        return self.exponent
#    def get_duration(self):
#        return self.speed
#    def get_congested_speed(self):
#        return self.congested_speed

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
def readData(clusterid, pudoid):
    #load trip data
    #load pudo locations
    #load clusters
    #load network nodes and edges
    ####this is a list of lists, each sublist is [time,o(lat,long),d(lat,long)]
    global tripList, tripWeightedNodes
    tripList = pd.read_csv('nodedTripList.csv').values.tolist()
    tripWeightedNodes = {}
    global nodeDict, edgeDict
    try:
        nodeDict, edgeDict
    except:
        print('reloading nodeDict and edgeDict')
        (nodeDict,edgeDict) = ATXxmlparse.getSDBNetworkTopo()
    for t in tripList:
        ###properly handling conversion of times past midnight###
        ###stripping trips without an associated time
        try:
            if len(t[0]) == 7:
                t[0] = '0'+t[0]
            if int(t[0][:2])>23:
                tfix = '01:0'+ str(int(t[0][:2])-23) + t[0][2:]
                t[0] = dt.datetime.strptime(tfix, '%d:%H:%M:%S')
            else:
                t[0] = dt.datetime.strptime(t[0], '%H:%M:%S')
        except:
            tripList.pop(0)
    tripList.sort(key=lambda x: x[0])
    tripWeightedNodes = tripweight_nodes(tripList)

    clusterList = pd.read_csv('CLUSTERS\\'+clusterid+'.csv'\
                              ,dtype = {'id':np.int\
                                        ,'lat':np.float64\
                                        ,'long':np.float64\
                                        ,'cluster':np.int})\
                            .values.tolist()
    PUDOList = pd.read_csv('PUDOS\\'+pudoid+'.csv'\
                           ,dtype = {'id':np.int\
                                     ,'lat':np.float64\
                                     ,'long':np.float64})\
                           .values.tolist()
    
    for c in clusterList:
        nodeDict[str(int(c[0]))].cluster = int(c[3])
    return PUDOList #(trips, PUDOList, nodeList, edgeList, clusterDict)

def createDataStructures(PUDOList, schedule):
    createNetwork()
    #nodeDict['3640042034'].cluster = 43
    #nodeDict['151437669'].cluster = 43
    global adjDict, PUDOs, enrouteDict,clusterPUDOs
    adjDict = getClusterAdjacencies()
    clusterPUDOs = {}
    for aj in adjDict.keys():
        clusterPUDOs[aj] = []
    PUDOs = buildPUDOs(PUDOList)
    for pu in PUDOs.values():
        clusterPUDOs[pu.get_cluster()].append(pu)
    schedule = createSchedule(schedule, config["numtrips"])
    enrouteDict = {}
    return PUDOs, schedule

def tripweight_nodes(trips):
    for n in nodeDict.keys():
        tripWeightedNodes[int(n)] = 0
    for t in trips:
        if t[0].hour < 2:
            tripWeightedNodes[t[1]] += 1
            #tripWeightedNodes[t[2]] -= 1
    #neg = min(list(tripWeightedNodes.values()))
    #for tr in tripWeightedNodes.keys():
    #    tripWeightedNodes[tr] -= neg
    total = sum(list(tripWeightedNodes.values()))
    for tr in tripWeightedNodes.keys():
        tripWeightedNodes[tr] = tripWeightedNodes[tr]/total
    return tripWeightedNodes

def createNetwork():
    print('building network')
    global ATXnet, ATXcongest
    load_gtraffic(edgeDict)
    ATXnet = nx.DiGraph()
    ATXcongest = nx.DiGraph()
    ATXnet.add_nodes_from(nodeDict.keys())
    ATXcongest.add_nodes_from(nodeDict.keys())
    for e in edgeDict.keys():
        if e[0] != e[1]:
            ATXnet.add_edge(e[0],e[1],weight=float(edgeDict[e].get_duration()))
            ATXcongest.add_edge(e[0],e[1],weight=float(edgeDict[e].get_congested_duration()))
    #return ATXnet
def initVehicles(PUDOs, numvehicles):
    plist = list(PUDOs.values())
    random_to_deploy = numvehicles%len(PUDOs)
    fixed_per_pudo = numvehicles//len(PUDOs)
    print('fixed and random', fixed_per_pudo, random_to_deploy)
    for p in PUDOs.values():
        p.capacity = fixed_per_pudo
    for n in range(random_to_deploy):
        np.random.choice(plist).capacity +=1

    return PUDOs
def buildPUDOs(PUDOList):
    print('building PUDOs')
    PUDOs = {}
    for p in PUDOList:
        ##########WORKING HERE ON DYNAMICALLY ASSIGNING VEHICLES TO PUDOS##########
        node = nodeDict[str(int(p[0]))]
        station = PUDO(node, node.get_cluster(),0)
        nodeDict[str(int(p[0]))].PUDO = station
        PUDOs[str(int(p[0]))] = station
    PUDOs = initVehicles(PUDOs, config["numvehicles"])
        #clusterPUDOs[node.get_cluster()].append(station)
        #PUDOs.append(PUDO(p[o], clusterDict[p[0]],[]))
        
        ####TEMP FOR DEBUGGING RIDESHARE####
#    nodeDict['2281'].PUDO.capacity = 0
#    writelist = []
#    for p in PUDOs.values():
#        writelist.append([p.get_ID(),p.get_capacity()])
#    pd.DataFrame(writelist).to_csv('debugpudos.csv')
    return PUDOs

    #get nodes in cluster
    #sum their weights
    #multiple by vehicle total in config
    #round it off
    #return it
        
def createSchedule(schedule, num):
    #print(num, 'trips being scheduled')
    ###walks through the triplist and adds trips to the schedule
    #def __init__(self, ID, hail_time, origin, destination, oPUDO, dPUDO):
    for t in tripList[:num]:
        #print('schedule',t)
        trip = tripList.pop(0)
        if str(trip[1]) in PUDOs.keys():
            opu = PUDOs[str(trip[1])]
        else:
            opu = findPUDO(nodeDict[str(trip[1])])[0]
        if str(trip[2]) in PUDOs.keys():
            dpu = PUDOs[str(trip[2])]
        else:
            dpu = findPUDO(nodeDict[str(trip[2])])[0]
        schedule[(trip[0],'Ride',trip[3])] = Event(trip[0], \
                                'Ride',\
                                Ride(trip[3],\
                                     trip[0],\
                                     str(trip[1]),\
                                     str(trip[2]),\
                                     opu,\
                                     dpu))
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
        adjDict[int(u[0])] = []
        adjDict[int(u[1])] = []
    for u in uniqAdj:
        if int(u[1]) not in adjDict[int(u[0])]:
            adjDict[int(u[0])].append(int(u[1]))
        if int(u[0]) not in adjDict[int(u[1])]:
            adjDict[int(u[1])].append(int(u[0]))
    for a in adjDict.keys():
        if a not in adjDict[a]:
            adjDict[a].append(a)
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
    #adjClusters = [node.get_cluster()]
    while len(nearbyPUDOs) == 0:
        adjClusters += [np.random.choice(adjDict[node.get_cluster()])]
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
    path = nx.astar_path(dynamic_net,str(origin),str(destination),weight='weight')
    distance = nx.astar_path_length(dynamic_net,str(origin),str(destination), weight='weight')
    return (path, distance)

def assignVehicle(ride, schedule): #,enrouteDict):
    oPUDO = ride.get_oPUDO()
    if oPUDO.get_capacity()>0:
        #if the default origin PUDO has a vehicle it is assigned to the ride
        oPUDO.capacity -= 1
        enrouteDict[(ride.get_hail_time(), ride.get_ID())] = ride
        schedule[(ride.get_arrival_time(),'Arrival',ride.get_ID())] = Event(ride.get_arrival_time(), 'Arrival',ride)
    else:
        #if not, we check ongoing rides for compatible rideshares
        print('no vehicle available, checking rideshare')
        sharing, schedule = rideshareLogic(ride, schedule)
        if sharing == False:
            print('no rideshare, pulling from adjacent PUDOs')
            schedule = findNearestVehicle(ride, schedule)
    return schedule
def findNearestVehicle(ride, schedule):
    nearbyPUDOs = [PUDOs[ride.get_origin()]]#clusterPUDOs[nodeDict[ride.get_origin()].get_cluster()]
        
    currlen = len(nearbyPUDOs)
    print('cluster',nodeDict[ride.get_origin()].get_cluster(),'len nearbypudos',len(nearbyPUDOs))
    while max(list(map(lambda nearbyPUDOs: nearbyPUDOs.get_capacity(), nearbyPUDOs))) == 0:
         print('DEBUG no vehicles at adj level',currlen,'@',ride.get_origin(),'extending search')
         
         newNodes = []
         p_to_add = []
         for newp in nearbyPUDOs:
             
             newNodes += list(dynamic_net[newp.get_ID()])
             #print('newp and newnodes',newp.get_ID(), len(newNodes))
             for n in newNodes:
                 if n in PUDOs.keys()\
                     and PUDOs[n] not in nearbyPUDOs\
                     and PUDOs[n] not in p_to_add:
                     p_to_add.append(PUDOs[n])
         print('adding these nodes to nearbyPUDOs', len(p_to_add))
         nearbyPUDOs += p_to_add
         currlen = len(nearbyPUDOs)          
         if currlen > 1000:
             print('no vehicle in nearest 1000 nodes, dropping ride')
             ride.dropped = True
             return schedule
#        if currlen > len(PUDOs)/2:
#            #more than half the system has been searched, terminating vehicle search and dropping ride
#            print('Dropping Ride')
#            ride.dropped = True
#            return schedule
#        for np in nearbyPUDOs[currlen:]:
#            currlen += 1
#            newPUDOs = findPUDO(nodeDict[np.get_ID()])
#            for new in newPUDOs:
#                if new not in nearbyPUDOs:
#                    nearbyPUDOs.append(new)
             
    #if no compatible rideshares are found we pull the PUDOs of the adjacent clusters
    
    #walk through adjacent PUDOs (in order of distance) and find nearest PUDO with capacity
    for np in nearbyPUDOs:
        if np.get_capacity() > 0:
            chosenPUDO = np
            break
#    #strip a vehicle from that PUDO and assign it, otherwise expand search
#    try:
    chosenPUDO.capacity -= 1
#    except:
#        print('no vehicles at adjacent PUDOs, expanding search')
#        print('DEBUG: ride is at',ride.get_origin())
#        #print('DEBUG: nearby PUDOs are:',list(map(lambda nearbyPUDOs: nearbyPUDOs.get_ID(), nearbyPUDOs)))
#        print('DEBUG:',list(map(lambda nearbyPUDOs: nearbyPUDOs.get_capacity(), nearbyPUDOs)))
#        nearbyPUDOs2 = []
#        for np2 in nearbyPUDOs:
#            nearbyPUDOs2 += findPUDO(nodeDict[np2.get_ID()])
#        print('DEBUG: near-nearby PUDOs are:',list(map(lambda nearbyPUDOs2: nearbyPUDOs2.get_ID(), nearbyPUDOs2)))
#        print('DEBUG:',list(map(lambda nearbyPUDOs2: nearbyPUDOs2.get_capacity(), nearbyPUDOs2)))
#        for np3 in nearbyPUDOs2:
#            if np3.get_capacity() > 0:
#                chosenPUDO = np3
#                break
#        chosenPUDO.capacity -= 1
     
    #recalculate the route and arrival time to include the vehicle travel
    #from the adjacent PUDO to the origin PUDO
    rideExt = shortestPath(chosenPUDO.get_ID(), ride.get_oPUDO().get_ID())
    ###[:-1]prevents duplicate node at the intersection
    ride.route = rideExt[0][:-1] + ride.get_route()
    ride.arrival_time = ride.get_arrival_time() + dt.timedelta(seconds = rideExt[1])
    #add the extended route to enroute trips and add the arrival to the schedule
    enrouteDict[(ride.get_hail_time(), ride.get_ID())] = ride
    schedule[(ride.get_arrival_time(),'Arrival',ride.get_ID())] = Event(ride.get_arrival_time(), 'Arrival',ride)            
    return schedule

def rideshareLogic(ride, schedule):
    oCluster = nodeDict[str(ride.get_origin())].get_cluster()
    dCluster = nodeDict[str(ride.get_destination())].get_cluster()
    rideClusters = []
    for nod in ride.get_route():
        ###build a list of clusters adjacent to the requested ride###
        rideClusters+=adjDict[nodeDict[nod].get_cluster()]
        ##rideClusters.append(nodeDict[nod].get_cluster()) 
    for rkey in enrouteDict.keys():
        ###for each of the routes currently enroute###
        enroute = enrouteDict[rkey]
        route = enroute.get_route()
        elapsed = ride.get_hail_time() - rkey[0]
        adjClusters = []
        timesum = dt.timedelta()
        #print('ROUTE DEBUG',route)
        vehicle_loc = route[0]
        finalCluster = nodeDict[str(route[-1:][0])].get_cluster()
        for pos in range(len(route)-1):
            ###identify the approximate location of the vehicle###
            ###by stepping through the route and assessing the travel time for each step###
            ###and comparing aggregate travel time to the elapsed time since the ride request###
            timesum += dt.timedelta(seconds=edgeDict[(route[pos],route[pos+1])].get_fft())
            if timesum < elapsed:
                ###the vehicle has already passed this node###
                ###shift the vehicles assumed location forward 1 node on its route###
                vehicle_loc = route[pos]
            if timesum > elapsed:
                ###the evaluated node has not yet been reached by the vehicle###
                ###add its adjacent clusters to our route cluster list###
                adjClusters += adjDict[nodeDict[str(route[pos])].get_cluster()]
        adjClusters = list(set(adjClusters))
        try:
            adjClusters.pop(adjClusters.index(finalCluster))
        except:
            pass
        if oCluster in adjClusters:
            ###if the ride originates in a cluster adjacent to the untraveled###
            ###portion of the route###
            if dCluster in adjClusters or nodeDict[route[-1:][0]].get_cluster() in rideClusters:
                ###If either the ride destination cluster lies adjacent to the untraveled portion###
                ###or the route destination lies adjacent to the requested rides route clusters###
#                print('found a compatible route', rkey)
                #remove existing arrival
                if 'reloc' in str(enroute.get_ID()):
                    schedule.pop((enroute.get_arrival_time(),'Reallocation',enroute.get_ID()))
                else:
                    schedule.pop((enroute.get_arrival_time(),'Arrival',enroute.get_ID()))
                
                #calculate route diversion to pick up new rider
                rideDiversion = shortestPath(vehicle_loc, ride.get_oPUDO().get_ID())
                #calculate new route and route extension, depending on whom is dropped off first
                if dCluster in adjClusters:
#                    print('within curr rout')
                    #if the current route contains both origin and destination the rideshare is dropped first
                    #new rider pickup to new ride destination
                    newRoute = shortestPath(ride.get_oPUDO().get_ID(), ride.get_dPUDO().get_ID())
                    #new ride destination to original route destination
                    rideExt = shortestPath(ride.get_dPUDO().get_ID(), route[-1:][0])
                    ride.arrival_time = ride.get_hail_time()+dt.timedelta(seconds = rideDiversion[1]+newRoute[1])
                    ###enrouteDict[rkey].arrival_time = ride.get_arrival_time() + dt.timedelta(seconds = rideExt[1])
                    ####enrouteDict[rkey].route = route[:pos] + rideDiversion[0][1:] + newRoute[0][1:] +rideExt[1:]
                    #pop current route from enrouteDict and recreate with proper keying
                    rt_arrival_time = ride.get_hail_time()+dt.timedelta(seconds = rideDiversion[1]+newRoute[1]+rideExt[1])
                    
                    ###TEMPORARILY REMOVING MULTISHARE (RIDESHARES ACCEPTING 3RDS)
                    ###enrouteDict[(rt_arrival_time), enroute.get_ID()] = enroute
                    ###enrouteDict[(rt_arrival_time), enroute.get_ID()].arrival_time = rt_arrival_time
                    ###enrouteDict[(rt_arrival_time), enroute.get_ID()].route = route[:route.index(vehicle_loc)+1] + rideDiversion[0][1:] + newRoute[0][1:] +rideExt[0][1:]
                    enroute.arrival_time = rt_arrival_time
                    enroute.route = route[:route.index(vehicle_loc)+1] + rideDiversion[0][1:] + newRoute[0][1:] +rideExt[0][1:]
                    enrouteDict.pop(rkey)
                    #create new schedule objects for rideshare and arrival
                    firstArrival = ride.get_hail_time()+dt.timedelta(seconds = rideDiversion[1]+newRoute[1])
                    finalArrival = firstArrival + dt.timedelta(seconds = rideExt[1])
                    schedule[(firstArrival,'Rideshare',ride.get_ID())] = Event(firstArrival,'Rideshare',ride)
                    schedule[(finalArrival, 'Arrival',enroute.get_ID())] = Event(finalArrival, 'Arrival',enroute)
                elif nodeDict[route[-1:][0]].get_cluster() in rideClusters:
#                    print('route extend')
                    #if the rideshare requires extending the current route the current rider is dropped first
                    
                    #new rider pickup to current route destination:
                    newRoute = shortestPath(ride.get_oPUDO().get_ID(), route[-1:][0])
                    #print('DEBUG curr route completed', route[:route.index(vehicle_loc)])
                    #print('DEBUG diversion', rideDiversion)
                    #print('DEBUG newroute',newRoute)
                    #print('DEBUG vehicle loc', vehicle_loc)
                    #current route destination to ride destination:
                    rideExt = shortestPath(route[-1:][0], ride.get_dPUDO().get_ID())
                    #print('DEBUG rideext',rideExt)
                    #pop current route from enrouteDict and recreate with proper keying
                    rt_arrival_time = ride.get_hail_time()+dt.timedelta(seconds = rideDiversion[1]+newRoute[1])
                    ###TEMPORARILY REMOVING MULTISHARE (RIDESHARES ACCEPTING 3RDS)
                    ###enrouteDict[(rt_arrival_time), enroute.get_ID()] = enroute
                    ###enrouteDict[(rt_arrival_time), enroute.get_ID()].arrival_time = rt_arrival_time
                    ###enrouteDict[(rt_arrival_time), enroute.get_ID()].route = route[:route.index(vehicle_loc)+1] + rideDiversion[0][1:] + newRoute[0][1:]
                    enroute.arrival_time = rt_arrival_time
                    enroute.route = route[:route.index(vehicle_loc)+1] + rideDiversion[0][1:] + newRoute[0][1:]
                    enrouteDict.pop(rkey)
                    #create new schedule objects for rideshare and arrival
                    ride.arrival_time = rt_arrival_time + dt.timedelta(seconds = rideExt[1])
                    ride.route = newRoute[0] + rideExt[0][1:]
                    firstArrival = ride.get_hail_time()+dt.timedelta(seconds = rideDiversion[1]+newRoute[1])
                    finalArrival = firstArrival + dt.timedelta(seconds = rideExt[1])
                    schedule[(firstArrival,'Rideshare',enroute.get_ID())] = Event(firstArrival,'Rideshare',enroute)
                    schedule[(finalArrival, 'Arrival',ride.get_ID())] = Event(finalArrival, 'Arrival', ride)
                return True, schedule
    return False, schedule   
#def newReallocate(schedule, currTime):
#    #if a PUDO has more vehicles at it than the config file PUDO capacity number, vehicles are redistributed to nearby PUDOs
#    for p in PUDOs.values():
#        if p.get_capacity > config["PUDOcap"]
def reallocateVehicles(schedule, currTime, relocnum):
    #print('reallocating vehicles')
    global vCount
    vCount = {}
    for c in adjDict.keys():
        vCount[c] = []
    #sum capacity of PUDOs in each cluster
    for p2 in PUDOs.values():
        adjacencies = adjDict[p2.get_cluster()]
        for a in adjacencies:
            vCount[a].append(p2.get_capacity())
    #count vehicles already enroute to the cluster
    for vs in vCount.values():
        vs.append(0)
    for e in enrouteDict.values():
        for aj in adjDict[e.get_dPUDO().get_cluster()]:
            vCount[aj][-1] += 1
    #convert vCount to a ratio of vehicles to PUDOs (normalize)
    vCount2 = {}
    for it in vCount.keys():
        ###list, sum vehicles, number of PUDOs (excl enroute entry in vcount), num onsite vehicles, v/p ttl, v/p onsite
        ###v/p total used to assess need, v/p onsite used to assess availability
        vCount2[it] = [vCount[it]\
                        , sum(vCount[it])\
                        , len(vCount[it])-1\
                        , sum(vCount[it][:-1])]
        vCount2[it].append(vCount2[it][1]/vCount2[it][2])
        vCount2[it].append(vCount2[it][3]/(vCount2[it][2]-1))
    needs_vehicles = []
    for macroCluster in vCount2.keys():
        if vCount2[macroCluster][4] < 1.5:
            ##shifted to 1.5 to check runtime
            needs_vehicles.append(macroCluster)
            ###this cluster needs vehicles###
            print('found an area that needs vehicles', macroCluster)
    for macroCluster in needs_vehicles:
        oPUDO = 'skip'        
        overCapCluster = max(vCount2.keys(), key=(lambda key: vCount2[key][5]))
        print('cluster with most available vehicles is', overCapCluster)
        for pp in PUDOs.values():
            if pp.get_cluster() in adjDict[overCapCluster] and pp.get_capacity() >= 1:
                #print('found opudo', pp.get_ID())
                #print(pp.get_cluster(), 'in', adjDict[overCapCluster])
                oPUDO = pp.get_ID()
                break
        #print(adjDict[macroCluster])
        for p in PUDOs.values():
            if p.get_cluster() in adjDict[macroCluster] and p.get_capacity() <= 1:
                #print('found dpudo',p.get_ID())
                #print(p.get_cluster(), 'in', adjDict[macroCluster])
                dPUDO = p.get_ID()
                break
        #print('moving vehicle from:',oPUDO,' to ', dPUDO)
        if oPUDO != 'skip':
            relocnum += 1        
            PUDOs[oPUDO].capacity -= 1
            ride = Ride('reloc'+str(relocnum), currTime, oPUDO, dPUDO, PUDOs[oPUDO], PUDOs[dPUDO])
            (ride.route, traveltime) = shortestPath(oPUDO, dPUDO)
            #print('DEBUG realloc traveltime',traveltime, oPUDO, dPUDO)
            ride.arrival_time = currTime + dt.timedelta(seconds = traveltime)
            #print('DEBUG for redundant schedule items', (ride.get_arrival_time(),'Reallocation',ride.get_ID()) in list(schedule.keys()))
            #print('DEBUG', (ride.get_arrival_time(),'Reallocation',ride.get_ID()))
            schedule[(ride.get_arrival_time(),'Reallocation',ride.get_ID())] = Event(ride.get_arrival_time(), 'Reallocation',ride)
            enrouteDict[(ride.get_hail_time(), ride.get_ID())] = ride
            #reduce vehicle count at overcap cluster by 1 and adjacent clusters
            for clus in adjDict[overCapCluster]:
                vCount2[clus][1] =- 1
                vCount2[clus][3] =- 1
                #recalc vehicles / pudo including excluding the enroute vehicles
                vCount2[clus][4] = vCount2[clus][1]/vCount2[clus][2]
                vCount2[clus][5] = vCount2[clus][3]/(vCount2[clus][2]-1)
    #print('no vehicles need to be reallocated')
    return schedule, relocnum
def masterEventHandler(event, schedule):
    #arrivals
    if event.get_eType() == 'Rideshare':
        #print('handling it')
        pass
        #run reporting for the event here
    elif event.get_eType() == 'Arrival' or event.get_eType() == 'Reallocation':
        #print('DEBUG EOBJ', event.get_eObj())
        event.get_eObj().dPUDO.capacity +=1
        #nodeDict[str(event.get_eObj().get_dPUDO().get_ID())].PUDO.capacity +=1
        try:
            enrouteDict.pop((event.get_eObj().get_hail_time(), event.get_eObj().get_ID()))
        except:
            print('rideshare vehicle, not in enroute dict')
        pass
        #add vehicle to destination PUDO capacity
        #log data from ride
    #relocates?
    #ride requests
    elif event.get_eType() == 'Ride':
        ride = event.get_eObj()
        ###calculate the default route for the ride
        path = shortestPath(ride.get_oPUDO().get_ID(), ride.get_dPUDO().get_ID())
        ride.route = path[0]
        ###calculate the default arrival time###
        ride.arrival_time = ride.get_hail_time() + dt.timedelta(seconds = path[1])
        schedule = assignVehicle(ride, schedule)#,enrouteList)
        #enrouteDict[ride.get_hail_time()] = ride.get_route()
        schedule = createSchedule(schedule, 1)
    else:
        print('unhandled event type')
        print(event.get_eType())
    if event.get_eObj().get_dropped() == True:
        event.eType = 'Dropped'
    eventReport(event, False, "","")
    return schedule
def eventReport(event, write, runid, idle):
    
    if write == True:
        out_df = pd.DataFrame(reporting, columns = ['time','type','ID','hail_time'\
                                              ,'arrival_time','vehicle_time','origin'\
                                              ,'destination','oPUDO','dPUDO','route'\
                                              ,'VMT','origin_walk','dest_walk','total_walk'])
        out_df.to_csv('REPORTING\\reporting_' + runid + '_v'+str(config["numvehicles"])+ '.csv')
        formatted_df = format_df(out_df)
        formatted_df.to_csv('REPORTING\\aggreport' + runid + '_v'+str(config["numvehicles"])+ '.csv')
        idle_df = pd.DataFrame(idle, columns = ['time','vehicles_at_PUDOs','vehicles_enroute','vehicles reallocating','num_rideshares'])
        idle_df["minute"] = idle_df.time.dt.hour*60 + idle_df.time.dt.minute
        idle_df = idle_df.groupby(['minute']).mean()
        idle_df.to_csv('REPORTING\\vreport_' + runid + '_v'+str(config["numvehicles"])+ '.csv')
        return reporting
    #if event.get_eType() in ['Ride','Arrival','Reallocation']
    obj = event.get_eObj()
    vehicle_time = obj.get_arrival_time() - obj.get_hail_time()
    walk_o_route = shortestPath(obj.get_origin(), obj.get_oPUDO().get_ID())[0]
    walkO = 0
    walkD = 0
    for wo in range(len(walk_o_route)-1):
        walkO += edgeDict[walk_o_route[wo], walk_o_route[wo+1]].get_length()
    walk_d_route = shortestPath(obj.get_destination(), obj.get_dPUDO().get_ID())[0]
    for wd in range(len(walk_d_route)-1):
        walkD += edgeDict[walk_d_route[wd], walk_d_route[wd+1]].get_length()
    ttl_walk = walkO + walkD
    VMT = 0
    route = obj.get_route()
    for n in range(len(route)-1):
        VMT += edgeDict[route[n],route[n+1]].get_length()
    out = [event.get_eTime()\
           ,event.get_eType()\
           ,obj.get_ID()\
           ,obj.get_hail_time()\
           ,obj.get_arrival_time()\
           ,vehicle_time
           ,obj.get_origin()\
           ,obj.get_destination()\
           ,obj.get_oPUDO().get_ID()\
           ,obj.get_dPUDO().get_ID()\
           ,route\
           ,VMT\
           ,walkO\
           ,walkD\
           ,ttl_walk\
           ]
    reporting.append(out)
def format_df(df):
    df['minute'] = df.time.dt.hour*60 + df.time.dt.minute
    #df = df[["minute","type"]]
    df = df.groupby(['minute','type']).mean()
    return df
def stateReport(verbose, schedule, idle, event):
    currEnroute = 0
    currRealloc = 0
    currRideshare = 0
    for sch in schedule.keys():
        if sch[1] == 'Arrival':
            currEnroute += 1 
        if sch[1] == 'Reallocation':
            currRealloc += 1
        if sch[1] == 'Rideshare':
            currRideshare += 1
    pv = 0
    for pp in PUDOs.values():
        if pp.get_capacity() < 0:
            print('capacity negative at', pp.get_ID())
            return (enrouteDict, schedule, event, idle) 
        pv += pp.get_capacity()
    ttl = pv + currEnroute + currRealloc
    if verbose == True:
        print('DEBUG: active, realloc, rideshare vehicles', currEnroute, currRealloc, currRideshare)
        print('vehicles at PUDOs',pv)
        print('total vehicles = ',ttl)
    idle.append([event.get_eTime(), pv, currEnroute, currRealloc, currRideshare])
    return pv, ttl, idle

def getNextEvent(schedule):
    #time = min(s, key=s.get.get_eOBJ
    time = min(list(schedule.keys()), key = lambda t: t[0])
    #delta_t = currTime - oldTime
    event = schedule.pop(time)
    return event, schedule#, delta_t

def simMaster():
    idle = []
    global reporting, config, dynamic_net
    reporting = []
    tripct = 0
    #global config
    relocnum = 0
    config = getConfig()
    runid = 'test_increase_relocate'
    #runid, ncluster = config["runids"][3], config["clustercounts"][6]
    clusterid, pudoid = 'clusterDict_kmean_nodeweight2000','network_PUDOsnoPUDO'
    PUDOList = readData(clusterid, pudoid)
    schedule = {}
    PUDOs, schedule = createDataStructures(PUDOList, schedule)
    init_time = dt.datetime.now()
    dynamic_net = ATXnet
    try:
        while len(schedule) > 0:
            elapsed = dt.datetime.now() - init_time
            tripct += 1
            if tripct > config["eventlimit"]:
                break
            event, schedule = getNextEvent(schedule)
            #print('')
            print(event.get_eTime(), event.get_eType(), event.get_eObj().get_ID(), 'elapsed:', elapsed)
            schedule = masterEventHandler(event, schedule)
            if event.get_eTime().hour in [7,8,16,17,18]:
                dynamic_net = ATXcongest
            else:
                dynamic_net = ATXnet

            pv, ttl, idle = stateReport(True, schedule, idle, event)
            if pv / len(PUDOs) > 2:
                #only reallocate if the number of idle vehicles is twice the number of PUDOs
                schedule, relocnum = reallocateVehicles(schedule, event.get_eTime(),relocnum)
            if ttl < config["numvehicles"]-1:
                print('missing vehicles, killing sim')
                return (enrouteDict, schedule, event, idle)

    except Exception as e:
        print(e)
        traceback.print_exc()
        return(enrouteDict, schedule, event, idle)
    eventReport(None, True, runid, idle)
    return (enrouteDict, schedule, None, idle)

        
    
    

#config = getConfig()
#(tripList,nodeDict,edgeDict,PUDOlist,clusterDict) = readData() 
#network = createNetwork(nodeDict,edgeDict)
#test = shortestPath(network, n[0],n[1])