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
import ATXxmlparse as ATXML
import vehicleSim
import pandas as pd

cwd = os.getcwd()
print(cwd)
with open(cwd+'\config.json') as f:
  config = json.load(f)

def Google_travel_time(edge):
    gmaps = gm.Client(key=config['api_key'])
    init_time = (datetime.datetime(2021,3,31,13,0) - datetime.datetime(1970,1,1,0,0)).total_seconds()
    #print(init_time)
    head = nodeDict[edgeDict[edge].get_head()]
    tail = nodeDict[edgeDict[edge].get_tail()]
    #1609480800 seconds since 1/1/1970 UCT
    result = gmaps.distance_matrix(head.get_coords_tup(),tail.get_coords_tup()\
                                   ,mode='driving'\
                                   , departure_time=init_time
                                   , traffic_model = 'pessimistic') #["rows"][0]["elements"][0]["distance"]["value"]
                                    
    origin = result['origin_addresses'][0]
    destination = result['destination_addresses'][0]
    distance = result['rows'][0]['elements'][0]['distance']['value']
    duration = result['rows'][0]['elements'][0]['duration']['value']
    try:
        duration_in_traffic = result['rows'][0]['elements'][0]['duration_in_traffic']['value']
    except:
        duration_in_traffic = duration
    speed_mph = 2.37 * distance / (duration_in_traffic + 1 )
    return [head.get_ID(),tail.get_ID(),origin,destination,distance,duration,duration_in_traffic,speed_mph]
        #print(speed_mph)
ii=0
donelist = []
outlist = pd.read_csv('googlemaps_data_full.csv').values.tolist()
for o in outlist:
    donelist.append((o[0],o[1]))
#for edge in edgeDict.keys():
    #broekn to prevent running
    ii+=1
    if (edge[0], edge[1]) in donelist:
        print('skipping edge',edge[0], edge[1])
    else:
        print('fetching edge',edge[0], edge[1])
        try:
            out = Google_travel_time(edge)
            outlist.append(out)
            #if ii > 100:
            #    break
            donelist.append((edge[0], edge[1]))
        except:
            print('cannot handle edge',edge[0], edge[1])
    if ii%1000 == 0:        
        odf = pd.DataFrame(outlist\
                           , columns = ['head','tail','origin_addr','dest_addr','distance','duration','traffic_duration','speed_mph']\
                           )
        odf.to_csv('googlemaps_data_full.csv', index = False)
    #for edge in edgeDict:


#(nodeDict, edgeDict) = ATXML.getNetworkTopo('')

#print(str(edgeDict[1].get_head()))       
#result = Google_travel_time(edgeDict)
#the coordinates need to be divided by 1000000