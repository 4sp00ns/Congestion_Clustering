# -*- coding: utf-8 -*-
"""
Created on Sat Oct  3 15:20:26 2020

@author: DAnderson
"""

#import leidenalg as la
#partition = la.find_partition(ATXnet, la.ModularityVertexPartition)
#import community as community_louvain

#import matplotlib.cm as cm
#import matplotlib.pyplot as plt
import networkx as nx
from networkx import community as c
<<<<<<< HEAD
from ATXxmlparse import getSDBNetworkTopo
import pandas as pd
from pudo_work import load_gtraffic
=======
from vehicleSim import createNetwork
from ATXxmlparse import getSDBNetworkTopo
>>>>>>> 85a521200f22f4c998ffe1517a11691fea358330
#louv = community_louvain.best_partition(ATXnet)
#louv_congest = community_louvain.best_partition(ATXcongest)

#import igraph
#elist = []
#fft_list = []
#congest_list = []
#for e in edgeDict.keys():
#    elist.append([int(e[0]),int(e[1])])
#    fft_list.append(edgeDict[e].get_duration())
#    congest_list.append(edgeDict[e].get_congested_duration())
#igraphATX = igraph.Graph(n=7466, edges=elist) #, edge_attrs={'weight':fft_list}) 
global nodeDict, edgeDict
(nodeDict, edgeDict) = getSDBNetworkTopo()
def createNetwork():
    print('building network')
    global ATXnet, ATXcongest, ATXnet_undir, ATXcongest_undir, dynamic_net
    load_gtraffic(edgeDict)
    ATXnet = nx.DiGraph()
    ATXnet_undir = nx.Graph()
    ATXcongest = nx.DiGraph()
    ATXcongest_undir = nx.Graph()
    ATXnet.add_nodes_from(nodeDict.keys())
    ATXcongest.add_nodes_from(nodeDict.keys())
    for e in edgeDict.keys():
        if e[0] != e[1]:
            ATXnet.add_edge(e[0],e[1],weight=float(edgeDict[e].get_duration()))
            ATXnet_undir.add_edge(e[0],e[1],weight=float(edgeDict[e].get_duration()))
            try:
                congested_dur = max([edgeDict[e].get_congested_duration(), edgeDict[e[::-1]].get_congested_duration()])
            except:
                congested_dur = edgeDict[e].get_congested_duration()
            ATXcongest.add_edge(e[0],e[1],weight=float(edgeDict[e].get_congested_duration()))
            ATXcongest_undir.add_edge(e[0],e[1],weight=float(congested_dur))
    dynamic_net = ATXnet
    #return ATXnet
createNetwork()

<<<<<<< HEAD
n_clusters = 500
#smallest_clique = 2
asyn = c.asyn_fluidc(ATXnet_undir, 500)
asyn_congest = c.asyn_fluidc(ATXcongest_undir,500)
=======
n_clusters = 7466
#smallest_clique = 2
asyn = c.asyn_fluidc(ATXnet_undir, 7466)
asyn_congest = c.asyn_fluidc(ATXcongest_undir,7466)
>>>>>>> 85a521200f22f4c998ffe1517a11691fea358330
##kclq = list(c.k_clique_communities(ATXnet,3))
##kclq_congest = list(c.k_clique_communities(ATXcongest, 3))
#
cl_out = {}
clcongest_out = {}
cl = 0
for nn in nodeDict.values():
    cl_out[nn.get_ID()] = [nn.get_lat(),nn.get_long()]
    clcongest_out[nn.get_ID()] = [nn.get_lat(),nn.get_long()]
for a in asyn:
    cl+=1
    for node in a:
        nout = nodeDict[node]
        cl_out[nout.get_ID()].append(cl)

cl2 = 0 
for sa in asyn_congest:
    cl2+=1
    for sa_node in sa:
        nout = nodeDict[sa_node]
        clcongest_out[nout.get_ID()].append(cl2)
pd.DataFrame.from_dict(cl_out,orient = 'index',columns = [\
                                          'lat'\
                                          ,'long'\
                                          ,'cluster'])\
            .to_csv('CLUSTERS\\clusterDict_async'\
                    +str(n_clusters)+'.csv'\
                , header = True)
pd.DataFrame.from_dict(clcongest_out,orient = 'index',columns = [\
                                          'lat'\
                                          ,'long'\
                                          ,'cluster'])\
            .to_csv('CLUSTERS\\clusterDict_asynccongest'\
                    +str(n_clusters)+'.csv'\
                , header = True)
            

#partition = community_louvain.best_partition(ATXnet)

# draw the graphk
#pos = nx.spring_layout(ATXnet)
## color the nodes according to their partition
#cmap = cm.get_cmap('viridis', max(partition.values()) + 1)
#nx.draw_networkx_nodes(ATXnet, pos, partition.keys(), node_size=4s,
#                       cmap=cmap, node_color=list(partition.values()))
#nx.draw_networkx_edges(ATXnet, pos, alpha=0.5)
#plt.show()