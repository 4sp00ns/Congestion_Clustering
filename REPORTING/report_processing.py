# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 21:51:40 2020

@author: Duncan
"""


import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, hour, when, dayofweek, avg, dayofyear, to_timestamp, minute, second
from matplotlib import pyplot as plt

spark = SparkSession.builder.getOrCreate()

#df = spark.read.options(header='True').csv('reporting_vehiclesensitivities_v25000.csv')

arr_df = df.filter(col('type') == 'Arrival')
 
arr_time = arr_df.select('vehicle_time')
split_col = pyspark.sql.functions.split(arr_time['vehicle_time'], 's ')
arr_time = arr_time.withColumn('trimtime', split_col.getItem(1))
arr_time = arr_time.withColumn('dt',to_timestamp(col('trimtime'),"HH:mm:ss"))
arr_time = arr_time.select('dt')
arr_time = arr_time.withColumn('hours',hour(col('dt')))
arr_time = arr_time.withColumn('minutes',minute(col('dt')))
arr_time = arr_time.withColumn('seconds',second(col('dt')))
arr_time = arr_time.withColumn('elapsed', col('hours')*60 + col('minutes') + col('seconds')/60)
arr_time = arr_time.select('elapsed')

listout = arr_time.toPandas().values.tolist()
l2 = []
for l in listout:
    l2.append(l[0])
    
