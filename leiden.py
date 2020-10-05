# -*- coding: utf-8 -*-
"""
Created on Sat Oct  3 15:20:26 2020

@author: DAnderson
"""

#import leidenalg as la
#partition = la.find_partition(ATXnet, la.ModularityVertexPartition)
import community as community_louvain
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import networkx as nx


#partition = community_louvain.best_partition(ATXnet)

# draw the graph
#pos = nx.spring_layout(ATXnet)
## color the nodes according to their partition
#cmap = cm.get_cmap('viridis', max(partition.values()) + 1)
#nx.draw_networkx_nodes(ATXnet, pos, partition.keys(), node_size=4s,
#                       cmap=cmap, node_color=list(partition.values()))
#nx.draw_networkx_edges(ATXnet, pos, alpha=0.5)
#plt.show()