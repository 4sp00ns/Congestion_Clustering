# -*- coding: utf-8 -*-
"""
Created on Mon Aug 24 21:14:50 2020

@author: Duncan.Anderson
"""

import csv
import json
import googlemaps as gm
import os
import datetime

cwd = os.getcwd()
print(cwd)
with open(cwd+'\config.json') as f:
  config = json.load(f)

class Node(object):
    def __init__(self, ID, lat, lng, edges):
        self.lat = lat
        self.lng = lng
        self.edges = edges
    def get_ID(self):
        return self.ID
    def get_lat(self):
        return self.lat
    def get_lng(self):
        return self.lng
    def get_edges(self):
        return self.edges
    def __str__(self):
        return str(self.lat) +','+ str(self.lng)
    def __hash__(self):
        return hash(str(self))
    def __eq__(self, other):
        return str(self) == str(other)
    
class Edge(object):
    def __init__(self, ID, head, tail, travel_time):
        self.head = head
        self.tail = tail
        self.travel_time = travel_time
    def get_ID(self):
        return self.ID
    def get_head(self):
        return self.head
    def get_tail(self):
        return self.tail
    def get_travel_time(self, hour):
        return self.travel_time[hour]
def Load_Data():   
    nodeDict = {}
    edgeDict = {}
    nodeObj = open(cwd+r'\austin_sdb_node.txt')
    netObj = open(cwd+r'\austin_sdb_net.txt')
    node_Read = csv.reader(nodeObj, delimiter="\t")
    net_Read = csv.reader(netObj, delimiter="\t")
    for row in node_Read:
        if row[0] != 'Node':
            ID = int(row[0])
            lat = int(row[2])/1000000
            lng = int(row[1])/1000000
            nodeDict[ID] = Node(ID,lat,lng,[])
    ID = 0
    for row in net_Read:
        ID+=1
        if ID > 7:
            tail = int(row[1])
            head = int(row[2])
            edgeDict[ID-7] = Edge(ID-7, nodeDict[head],nodeDict[tail],[])
    return (nodeDict, edgeDict)

def Google_travel_time(edgeDict):
    timetest = []
    gmaps = gm.Client(key=config['api_key'])
    init_time = (datetime.datetime(2021,4,1,17,0) - datetime.datetime(1970,1,1,0,0)).total_seconds()
    for edge in range(1,len(edgeDict)):
        #1609480800 seconds since 1/1/1970 UCT
        for t in range(0,1):
            result = gmaps.distance_matrix(str(edgeDict[edge].get_head()), str(edgeDict[edge].get_tail()),\
                                           mode='driving', departure_time=init_time + t*15*60) #["rows"][0]["elements"][0]["distance"]["value"]
            print(result) 
            distance = result['rows'][0]['elements'][0]['distance']['value']
            duration = result['rows'][0]['elements'][0]['duration']['value']
            duration_in_traffic = result['rows'][0]['elements'][0]['duration_in_traffic']['value']
            speed_mph = 2.37 * distance / (duration_in_traffic + 1 )
            edgeDict[edge].travel_time.append(duration_in_traffic)
            #distance over time for rate (speed) is better congestion metric
            #pull rush hour time for all edges and look for slowest speed to find congestion and then test time series
            #we need to test time series because right now its returning flat values
            timetest.append(speed_mph)
            print(speed_mph)
    print(timetest)
    #for edge in edgeDict:
    return timetest
(nodeDict, edgeDict) = Load_Data()
print(str(edgeDict[1].get_head()))       
result = Google_travel_time(edgeDict)
#the coordinates need to be divided by 1000000