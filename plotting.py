# -*- coding: utf-8 -*-
"""
Created on Sat Oct 17 18:02:53 2020

@author: DAnderson
"""

# draw the graphk
#pos = nx.spring_layout(ATXnet)
## color the nodes according to their partition
#cmap = cm.get_cmap('viridis', max(partition.values()) + 1)
#nx.draw_networkx_nodes(ATXnet, pos, partition.keys(), node_size=4s,
#                       cmap=cmap, node_color=list(partition.values()))
#nx.draw_networkx_edges(ATXnet, pos, alpha=0.5)
#plt.show()

##write output networks
##nx.write_graphml(ATXnet, 'atxnet_vis.graphml')
from numpy import random

def random_color():
    levels = range(32,256,32)
    return tuple(random.choice(levels) for _ in range(3))

print('building network')
global ATXvis
#load_gtraffic(edgeDict)
#30.269673, -97.739134
#1827, 462, 1016
clustersToMap = adjDict[1827]+adjDict[462]+adjDict[1016]

colorDict = {195:'red'\
             ,331:'orange'\
             ,462:'yellow'\
             ,633:'green'\
             ,1016:'blue'\
             ,1142: 'purple'\
             ,1639: 'white'\
             ,1827: 'brown'\
             ,1985: 'black'}

ATXvis = nx.DiGraph()
nodesToMap = []
for n in nodeDict.keys():
    if nodeDict[n].get_cluster() in clustersToMap:
        nodesToMap.append(n)
for ntm in nodesToMap:
    ATXvis.add_node(ntm\
                    , cluster = nodeDict[ntm].get_cluster()\
                    , color=colorDict[nodeDict[ntm].get_cluster()]\
                    , lat = nodeDict[ntm].get_lat()*1000\
                    , long = nodeDict[ntm].get_long()*1000)
for e in edgeDict.keys():
    if e[0] != e[1] and e[0] in nodesToMap and e[1] in nodesToMap:
        eObj = edgeDict[e]
        ATXvis.add_edge(e[0],e[1]\
                        ,weight=float(eObj.get_duration())\
                        ,congestion=eObj.get_congested_duration() / eObj.get_duration())

nx.write_graphml(ATXvis, 'atxnet_vis_mini_coordexp_cong.graphml')