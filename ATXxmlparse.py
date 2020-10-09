# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 10:05:04 2020

@author: Duncan
"""

from xml.dom import minidom
import csv
import pyproj
import numpy as np
import networkx as nx
import pandas as pd

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
    def get_coords_tup(self):
        return (self.lat,self.long)
    def get_coords_str(self):
        return (str(self.lat)+','+str(self.long))
    
class Edge(object):
    def __init__(self, ID, tail, head, capacity, length, fft, b, exponent, duration):
        self.ID = ID
        self.head = head
        self.tail = tail
        self.capacity = capacity
        self.length = length
        self.fft = fft
        self.b = b
        self.exponent = exponent
        self.duration = duration
        self.congested_duration = fft
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
    def get_duration(self):
        return self.duration
    def get_congested_duration(self):
        return self.congested_duration

def createTransformer():
    return pyproj.Transformer.from_crs("epsg:2958", "epsg:4326")
def getTrips(path):
    try:
        print('loading existing triplist')
        triplist = pd.read_csv('tripData.csv').values.tolist()
    except:
        print('reloading trip xml')
        transformer = createTransformer()
        try:
            tripdata = minidom.parse(r'F:\Austin_Multimodal\revised_austin_plans\revised_austin_plans.xml')
        except:
            pass
        try:
            tripdata = minidom.parse(r'D:\Austin_Multimodal\revised_austin_plans\revised_austin_plans.xml')
        except:
            pass
        people = tripdata.getElementsByTagName('person')
        
        triplist = []
        for p in people:
            action = p.getElementsByTagName('activity')
            leg = p.getElementsByTagName('leg')
            for aa in range(0,len(action)-1):
                time = action[aa].getAttribute('end_time')
                (origin_lat, origin_long) = transformer.transform(float(action[aa].getAttribute('x')), float(action[aa].getAttribute('y')))
                (dest_lat, dest_long) = transformer.transform(float(action[aa+1].getAttribute('x')), float(action[aa+1].getAttribute('y')))
                if time != '':
                    triplist.append([time,origin_lat,origin_long,dest_lat,dest_long])

    #output = np.asarray(triplist)
    #np.savetxt('ATXtrips.csv', output, delimiter=',')
    return triplist
   
def getNetworkTopo(path):
    transformer = createTransformer()
    nodeout = {}
    edgeout = {}
    try:
        networkData = minidom.parse(r'F:\Austin_Multimodal\austin_multimodalnetwork\austin_multimodalnetwork.xml')
    except:
        pass
    try:
        networkData = minidom.parse(r'D:\Austin_Multimodal\austin_multimodalnetwork\austin_multimodalnetwork.xml')
    except:
        pass
    nodes = networkData.getElementsByTagName('nodes')[0].getElementsByTagName('node')
    links = networkData.getElementsByTagName('links')[0].getElementsByTagName('link')
    for n in nodes:
        coords = transformer.transform(float(n.getAttribute('x')), float(n.getAttribute('y')))
        nodeout[n.getAttribute('id')]= Node(n.getAttribute('id')\
                                           ,coords[0]\
                                           ,coords[1]\
                                           ,0)
    for l in links:
        edgeout[(l.getAttribute('from'),l.getAttribute('to'),l.getAttribute('length'))] = Edge(l.getAttribute('id')\
                                                                     ,l.getAttribute('from')\
                                                                     ,l.getAttribute('to')\
                                                                     ,l.getAttribute('length')\
                                                                     ,[])
    df = pd.DataFrame.from_dict(nodeout, orient="index")
    df.to_csv("nodeDict.csv")
    df = pd.DataFrame.from_dict(edgeout, orient="index")
    df.to_csv("edgeDict.csv")
    return (nodeout,edgeout)

def getSDBNetworkTopo():
    nodeout = {}
    edgeout = {}
    import os
    cwd = os.getcwd()
    with open('Austin_sdb_net.txt') as networkFile:
        fileLines = networkFile.read().splitlines()[7:]
        for line in fileLines:
            data = line.split()
            edgeout[data[0],data[1]] = Edge((data[0],data[1])\
                                             , data[0]\
                                             , data[1]\
                                             , float(data[2])\
                                             , float(data[3])\
                                             , float(data[4])\
                                             , float(data[5])\
                                             , float(data[6])\
                                             , float(data[7]))
    with open('Austin_sdb_node.txt') as nodeFile:
        fileLines = nodeFile.read().splitlines()[1:]
        for line in fileLines:
            data = line.split()
            nodeout[data[0]] = Node(data[0], int(data[2])/1000000, int(data[1])/1000000, 0)
    return (nodeout, edgeout)

def create_network(nodes,links):
    ATXgraph = nx.Graph()
    ATXgraph.add_nodes_from(nodes.keys())
    ATXgraph.add_edges_from(links.keys())
    return ATXgraph


#with open(r'F:\Austin_Multimodal\testcsv.csv', 'w') as myfile:
#    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
#    wr.writerow(triplist)                
#print(items)
# one specific item attribute
#print('Item #2 attribute:')
#print(items[0].attributes['dep_time'].value)


# all item attributes
#print('\nAll attributes:')
#for elem in items:
#    print(elem.attributes['dep_time'].value)

# one specific item's data
#print('\nItem #2 data:')
#print(items[1].firstChild.data)
#print(items[1].childNodes[0].data)

# all items data
#print('\nAll item data:')
#for elem in items:
#    print(elem.firstChild.data)