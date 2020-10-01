# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 18:58:51 2020

@author: DAnderson
"""
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
    


import os
cwd = os.getcwd()
with open(cwd+r'\network_data\Austin_net.tntp.txt') as networkFile:
    fileLines = networkFile.read().splitlines()[9:]
    for line in fileLines:
        data = line.split()
        
    