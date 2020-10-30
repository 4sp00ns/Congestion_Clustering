# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 21:46:08 2020

@author: DAnderson
"""
from matplotlib import pyplot as plt
import numpy
import networkx as nx
congestionRatios = {}
outD = {}
def createRatioDicts():
    for node in nodeDict.keys():
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
    congestionRatios.pop('6781')
    for node in congestionRatios.keys():
        outD[node] = numpy.std(congestionRatios[node])
        outD2[node] = sum(congestionRatios[node])/len(congestionRatios[node])
        outD3[node] = max(congestionRatios[node])/min(congestionRatios[node])
    outL = np.array(list(outD.values()))
    outL2 = np.array(list(outD2.values()))
    outL3 = np.array(list(outD3.values()))
    
    plt.hist(outL,bins=20, color='blue', label='stdev')
    plt.hist(outL2, bins=20, color='red', label='mean_ratio')
    plt.hist(outL3, bins=20,color='green', label='range')
    return outD, outD2, outD3
def choosePUDOs(numpudos, ratioDict, typename):
    newPUDOs = []
    for num in range(numpudos):
        nodeID = max(ratioDict, key=ratioDict.get)
        ratioDict.pop(nodeID)
        print(nodeID)
        newPUDOs.append([int(nodeID), nodeDict[nodeID].get_lat(), nodeDict[nodeID].get_long()])
    pd.DataFrame(newPUDOs, columns = ['id','lat','long'])\
                        .to_csv('PUDOS\\networkPUDOs_novel_'+typename+str(numpudos)+'.csv'\
                                ,index=False)
    return newPUDOs

#stdDict, meanDict, rngDict = createRatioDicts()
def residualPUDOs(newPUDOs):
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

def build_hybrid_pudos(current_pudofile, current_clusterfile):
    PUDOList = pd.read_csv('PUDOS\\'+current_+pudofile+'.csv'\
                       ,dtype = {'id':np.int\
                                 ,'lat':np.float64\
                                 ,'long':np.float64})\
                       .values.tolist()
    clusterList = pd.read_csv('CLUSTERS\\'+current_clusterfile+'.csv'\
                          ,dtype = {'id':np.int\
                                    ,'lat':np.float64\
                                    ,'long':np.float64\
                                    ,'cluster':np.int})\
                        .values.tolist()
    urban_core = pd.read_csv('urban_core_nodes.csv').values.tolist()
    
    