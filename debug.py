# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 22:27:45 2020

@author: Duncan
"""
#speeds = []
#for e in edgeDict.values():
#    length = e.get_length()
#    duration = e.get_duration()
#    congested = e.get_congested_duration()
#    if duration > 0:
#        mph = (length/5280) / (duration/60/60)
#        speeds.append(mph)

traffic = pd.read_csv('googlemaps_data_full.csv').values.tolist()    

import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, hour, when, dayofweek, avg, dayofyear, to_timestamp, minute, second
from matplotlib import pyplot as plt

spark = SparkSession.builder.getOrCreate()

traffic = spark.read.options(header='True').csv('googlemaps_data_full.csv')