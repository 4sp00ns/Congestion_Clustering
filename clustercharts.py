# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 13:11:39 2020

@author: duncan.anderson
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 12:12:05 2020

@author: duncan.anderson
"""
import os
os.chdir('C:\\Users\\duncan.anderson\\Congestion_Clustering')
import pandas as pd
import numpy as np
import networkx as nx
import ATXxmlparse
from matplotlib import pyplot as plt
def clustersizehist(clusterfile):
    clusterList = pd.read_csv('CLUSTERS\\'+clusterfile+'.csv'\
                                  ,dtype = {'id':np.int\
                                            ,'lat':np.float64\
                                            ,'long':np.float64\
                                            ,'cluster':np.int})
    #clusterList = clusterList['cluster']
    for c in clusterList.values.tolist():
        print(c[0])
        nodeDict[str(int(c[0]))].cluster = int(c[3])
    clusterList.groupby('cluster')['cluster'].count().plot.hist(bins=10)
    plt.title('Congested Network Cluster Size Distribution')
    plt.xlabel('Cluster Size')
    return clusterList.groupby('cluster')['cluster'].count()
    


#
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
    for a in adjDict.keys():
        adjDict[a] = list(set(adjDict[a]))
    return adjDict

def build_cluster_graph(ctdict):
    ATXcluster = nx.Graph()
    for aj in adjDict.keys():
        ATXcluster.add_node(aj\
                            , size = ctdict[aj]\
                            , lat = nodeDict[str(aj)].get_lat()*1000\
                            , lng = nodeDict[str(aj)].get_long()*1000)
    for aj in adjDict.keys():
        for adj in adjDict[aj]:
            if aj != adj:
                ATXcluster.add_edge(aj, adj)
    return ATXcluster
        
def master(clustfile):
    global nodeDict, edgeDict
    (nodeDict, edgeDict) = ATXxmlparse.getSDBNetworkTopo()
    clusterList = clustersizehist(clustfile).values.tolist()
    adjDict = getClusterAdjacencies()
    clusd = {}
    #print(clusterList)
    ct = 1
    for cl in clusterList:
        #print('adding cluster', cl)
        clusd[ct] = cl
        ct+=1
    #print(clusd)
    ATXcluster = build_cluster_graph(clusd)
    nx.write_graphml(ATXcluster, clustfile+'.graphml')
# ATXnet.add_nodes_from(nodeDict.keys())
#     ATXcongest.add_nodes_from(nodeDict.keys())
#     for e in edgeDict.keys():
#         if e[0] != e[1]:
#             ATXnet.add_edge(e[0],e[1],weight=float(edgeDict[e].get_duration()))
#             ATXnet_undir.add_edge(e[0],e[1],weight=float(edgeDict[e].get_duration()))
#             try:
#                 congested_dur = max([edgeDict[e].get_congested_duration(), edgeDict[e[::-1]].get_congested_duration()])
#             except:
#                 congested_dur = edgeDict[e].get_congested_duration()
#             ATXcongest.add_edge(e[0],e

