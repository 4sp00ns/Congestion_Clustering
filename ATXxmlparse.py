# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 10:05:04 2020

@author: Duncan
"""

from xml.dom import minidom
import utm
import csv
import pyproj
import numpy as np


transformer = pyproj.Transformer.from_crs("epsg:2958", "epsg:4326")

mydoc = minidom.parse(r'F:\Austin_Multimodal\revised_austin_plans\revised_austin_plans.xml')

people = mydoc.getElementsByTagName('person')

triplist = []
for p in people:
    action = p.getElementsByTagName('activity')
    leg = p.getElementsByTagName('leg')
    for aa in range(0,len(action)-1):
        time = action[aa].getAttribute('end_time')
        origin = str(transformer.transform(float(action[aa].getAttribute('x')), float(action[aa].getAttribute('y'))))
        destination = str(transformer.transform(float(action[aa+1].getAttribute('x')), float(action[aa+1].getAttribute('y'))))
        triplist.append([time,origin,destination])

output = np.asarray(triplist)
np.savetxt('ATXtrips.csv', output, delimiter=',')

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