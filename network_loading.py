# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 18:58:51 2020

@author: DAnderson
"""
import os
cwd = os.getcwd()
with open(cwd+r'\network_data\Austin_net.tntp.txt') as networkFile:
    fileLines = networkFile.read().splitlines()[9:]
    for line in fileLines:
        data = line.split()
        print(data[3])
    