# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 21:46:08 2020

@author: DAnderson
"""
from matplotlib import pyplot as plt
import numpy
import networkx as nx
from pudo_work import bruteCoord, run_kmeans, load_gtraffic
import ATXxmlparse
import numpy as np
import pandas as pd

global nodeDict, edgeDict
(nodeDict,edgeDict) = ATXxmlparse.getSDBNetworkTopo()
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

def createRatioDicts(urban_core_nodes):
    congestionRatios = {}
    outD = {}
    outD2 = {}
    outD3 = {}
    for node in nodeDict.keys():
        #if [int(node), nodeDict[node].get_lat(), nodeDict[node].get_long()] in urban_core_nodes:
        #print(node)
        adjnodes = []
        congestionRatios[node] = []
        neighbors = list(ATXnet.neighbors(node))
        for n in neighbors:
            adjnodes += list(ATXnet.neighbors(n))
        adjUnique = list(set(adjnodes))
        while node in adjUnique:
            adjUnique.remove(node)
        for n in neighbors:
            while n in adjUnique:
                adjUnique.remove(n)
    #    for e in edgeDict.keys():
    #        if e[0] == node:
    #            adjnodes.append(e[1])
    #        if e[1] == node:
    #            adjnodes.append(e[0])
        for oNode in adjUnique:
            ratio = 1
            try:
                dur = nx.astar_path_length(ATXnet,oNode,node,weight='weight')
                cdur = nx.astar_path_length(ATXcongest,oNode,node,weight='weight')
                ratio = cdur / dur
            except:
                print('no route possible')
    #    for an in adjnodes:
    #        for e in edgeDict.keys():
    #            if e[0] == an or e[1] == an:
    #                edge = edgeDict[e]
    #                if edge.get_duration() == 0 or edge.get_congested_duration() == 0:
    #                    pass
    #                else:
    #                    #print('real val')
            congestionRatios[node].append(ratio)
    #congestionRatios.pop('6781')
    for node in congestionRatios.keys():
        #outD[node] = numpy.std(congestionRatios[node])
        #outD2[node] = sum(congestionRatios[node])/len(congestionRatios[node])
        outD3[node] = max(congestionRatios[node])/min(congestionRatios[node])
    #outL = np.array(list(outD.values()))
    #outL2 = np.array(list(outD2.values()))
    outL3 = np.array(list(outD3.values()))
    
    #plt.hist(outL,bins=20, color='blue', label='stdev')
    #plt.hist(outL2, bins=20, color='red', label='mean_ratio')
    plt.hist(outL3, bins=20,color='green', label='range')
    return outD3
def choosePUDOs(numpudos, ratioDict, typename):
    #DEPRECATED?######
    newPUDOs = []
    for num in range(numpudos):
        nodeID = max(ratioDict, key=ratioDict.get)
        ratioDict.pop(nodeID)
        #print(nodeID)
        newPUDOs.append([int(nodeID), nodeDict[nodeID].get_lat(), nodeDict[nodeID].get_long()])
    pd.DataFrame(newPUDOs, columns = ['id','lat','long'])\
                        .to_csv('PUDOS\\networkPUDOs_novel_'+typename+str(numpudos)+'.csv'\
                                ,index=False)
    return newPUDOs


def num_UC_clusters(clusterfile):
    uc = pd.read_csv('urban_core_nodes.csv', dtype = {'id':np.str\
                                    ,'lat':np.float64\
                                    ,'long':np.float64}).values.tolist()
    df_cl = pd.read_csv('CLUSTERS\\'+clusterfile+'.csv'\
                          ,dtype = {'id':np.str\
                                    ,'lat':np.float64\
                                    ,'long':np.float64\
                                    ,'cluster':np.str})
    #print(df_cl.columns)
    clusterD = {}
    #df_cl['id'] = df_cl['Unnamed']
    clusterList = df_cl[['id','cluster']].values.tolist()
    for cl in clusterList:
        clusterD[cl[0]] = cl[1]
    clustout = []
    for nn in uc:
        clustout.append(clusterD[nn[0]])
    clustout = list(set(clustout))
    #print(clustout)
    return clustout
#stdDict, meanDict, rngDict = createRatioDicts()
    

def residualPUDOs(newPUDOs):
    #finds distant PUDOs
    ct = 0
    for n in nodeDict.keys():
        mindist = 99999
        cluster = nodeDict[n].get_cluster()
        adjc = adjDict[cluster]
        #nlat = nodeDict[n].get_lat()
        #nlng = nodeDict[n].get_long()
        for np in newPUDOs:
            if type(np) == list:
                np = np[0]
            pcluster = nodeDict[str(np)].get_cluster()
            if pcluster in adjc:
                try:
                    dist = nx.astar_path_length(ATXnet,str(n),str(np),weight='length')
                    if dist < mindist:
                        mindist = dist
                except:
                    pass
        if mindist > 1300:
            print('no PUDO within half a mile',n)
            ct+=1
        else:
            print('PUDOs close enough',n)
    return ct
            #plat = nodeDict[str(np[0])].get_lat()
            #plng = nodeDict[str(np[0])].get_long()
        
def distantNodes():
    #finds distant nodes
    ct = []
    for n in nodeDict.keys():
        print(n)
        lenlist=[]
        elist = list(ATXnet.edges(n))
        node = nodeDict[n]
        for e in elist:
            lenlist.append(edgeDict[e].get_length())
        if min(lenlist) > 1300:
            ct.append([node.get_ID(), node.get_lat(), node.get_long(),"not"])
        else:
            ct.append([node.get_ID(), node.get_lat(), node.get_long(),"PUDO Eligible"])
    pd.DataFrame(ct, columns=['id','lat','long','pudo'])\
                    .to_csv('PUDOS\\eligible_nodes.csv',index=False)
    return ct
        
def reducedNodes():
    #defines the urban core and writes it to csv
    outL = []
    outN = []
    edgect = 0
    tripct = 0
    for n in nodeDict.keys():
        node = nodeDict[n]
        if node.get_lat() > 30.25 and node.get_lat() < 30.29:
            if node.get_long() < -97.73 and node.get_long() > -97.75:
                outL.append([n, node.get_lat(), node.get_long()])
                outN.append(n)
    for e in edgeDict.values():
        if e.get_head() in outN:
            edgect+=1
    for t in tripList:
        if str(t[1]) in outN or str(t[2]) in outN:
            tripct+=1
    pd.DataFrame(outL, columns=['id','lat','long'])\
                .to_csv('urban_core_nodes.csv')
    return outL, edgect, tripct

def build_hybrid_pudos(current_clusterfile, urban_core, rangeD):
#    PUDOList = pd.read_csv('PUDOS\\'+current_+pudofile+'.csv'\
#                       ,dtype = {'id':np.int\
#                                 ,'lat':np.float64\
#                                 ,'long':np.float64})\
#                       .values.tolist()
    clusterList = pd.read_csv('CLUSTERS\\'+current_clusterfile+'.csv'\
                          ,dtype = {'id':np.str\
                                    ,'lat':np.float64\
                                    ,'long':np.float64\
                                    ,'cluster':np.str})\
                        .values.tolist()
    print(clusterList[0])
    print(urban_core[0:5])
    #urban_core = pd.read_csv('urban_core_nodes.csv').values.tolist()
    core_cluster = []
    newPUDOs = []
    trunc_cl = []
    for cL in clusterList:
        if cL[:3] in urban_core:
            #print('dingdingding')
            core_cluster.append(cL[3])
            core_cluster = list(set(core_cluster))
            trunc_cl.append(cL)
    for cL in clusterList:
        if str(cL[3]) not in core_cluster:
            newPUDOs.append(cL[:3])
    print('DEBUG: len core clusters',len(core_cluster))
    print(len(newPUDOs))
    if 'kmean' not in current_clusterfile:
        print('non kmean cluster')
        centroidPUDOs, congestPUDOs = define_PUDOs(newPUDOs, trunc_cl, rangeD)
    elif 'kmean' in current_clusterfile:
        print('kmean clusters')
        kmeanfix = count_correct_kmeans_PUDOS(current_clusterfile.replace('kmean_nodeweight','asynccongest'))
        print('DEBUG',kmeanfix[0])
        centroidPUDOs, congestPUDOs = define_PUDOs(newPUDOs, kmeanfix, rangeD)
    pd.DataFrame(centroidPUDOs, columns=['id','lat','long'])\
            .to_csv('PUDOS\\PUDOs_UCentroid_'+current_clusterfile[12:]+'.csv', index=False)
    pd.DataFrame(congestPUDOs, columns=['id','lat','long'])\
            .to_csv('PUDOS\\PUDOs_UCongest_'+current_clusterfile[12:]+'.csv', index=False)
    return centroidPUDOs, congestPUDOs

def define_PUDOs(newPUDOs, clusterList, ratioDict):
    print('DEBUG CLUSTERLIST', clusterList[0:3])
    centroidPUDOs = newPUDOs.copy()
    congestPUDOs = newPUDOs.copy()
    clusterNodes = {}
    centDict = {}
    clusDict = {}
    for cl in clusterList:
        clusterNodes[str(cl[3])] = []
    for cl in clusterList:
        clusterNodes[str(cl[3])].append(nodeDict[str(cl[0])])
    print('number of PUDOS to build',len(clusterNodes))
    for nl in clusterNodes.values():
        mrat = 0
        for nodelet in nl:
            if ratioDict[nodelet.get_ID()] > mrat:
                mrat = ratioDict[nodelet.get_ID()]
                max_r_node = nodelet
        ######these lambda functions still dont work
        #max_r_node = max(list(map(lambda nl: ratioDict[nl.get_ID()]))))
        #max(list(map(lambda nl: ratioDict[nl.get_ID()], nl)))
        #print(max_r_node)
        sum_lat = sum(list(map(lambda nl: nl.get_lat(), nl)))
        sum_long = sum(list(map(lambda nl: nl.get_long(), nl)))
        centroid_lat = sum_lat / len(nl)
        centroid_long = sum_long / len(nl)
        reduced_nodes = {}
#        curclus = nl.get_cluster()
#        for n in nodeDict.keys():
#            if nodeDict[n].get_cluster() == curclus:
#                reduced_nodes[n] = nodeDict[n]
        for nodelet in nl:
            reduced_nodes[nodelet.get_ID()] = nodelet 
        centroid_node_id = bruteCoord((centroid_lat, centroid_long), reduced_nodes)
        centroid_node = nodeDict[centroid_node_id]
        centroidPUDOs.append([centroid_node.get_ID(), centroid_node.get_lat(), centroid_node.get_long()])
        congestPUDOs.append([max_r_node.get_ID(), max_r_node.get_lat(), max_r_node.get_long()])
    return centroidPUDOs, congestPUDOs

def test_async():
    clusterList1 = pd.read_csv('CLUSTERS\\'+'clusterDict_asynccongest1000'+'.csv'\
                      ,dtype = {'id':np.str\
                                ,'lat':np.float64\
                                ,'long':np.float64\
                                ,'cluster':np.str})\
                    .values.tolist()
    clusterList2 = pd.read_csv('CLUSTERS\\'+'clusterDict_asynccongest2000'+'.csv'\
                      ,dtype = {'id':np.str\
                                ,'lat':np.float64\
                                ,'long':np.float64\
                                ,'cluster':np.str})\
                    .values.tolist()
    cl1 = []
    cl2 = []
    #for uc in urban_core:
     #   if 
     
def count_correct_kmeans_PUDOS(asyncclusterfile):
    uc_df = pd.read_csv('urban_core_nodes.csv', dtype = {'id':np.str\
                                    ,'lat':np.float64\
                                    ,'long':np.float64})
    urban_core_ll = uc_df[['lat','long']].values.tolist()
    urban_core = uc_df.values.tolist()
    numToCreate = len(num_UC_clusters(asyncclusterfile))
    print(numToCreate)
    kmd = run_kmeans(urban_core_ll, numToCreate)
    cluslist = kmd[1].tolist()
    ctt = 0
    for uc in urban_core:
        uc.append(cluslist[ctt])
        ctt+=1
    return urban_core
     
     
def master(clusterfile):
    urban_core = pd.read_csv('urban_core_nodes.csv', dtype = {'id':np.str\
                                    ,'lat':np.float64\
                                    ,'long':np.float64}).values.tolist()
    ratioDict = createRatioDicts(urban_core)
    build_hybrid_pudos(clusterfile, urban_core, ratioDict)
    
createNetwork()