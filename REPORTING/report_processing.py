# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 21:51:40 2020

@author: Duncan
"""


import json
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, year, month, hour, when, dayofweek\
, avg, dayofyear, to_timestamp, minute, second, lit, count
from matplotlib import pyplot as plt
import pandas as pd
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

#df = spark.read.options(header='True').csv('reporting_vehiclesensitivities_v25000.csv')

#arr_df = df.filter(col('type') == 'Arrival')
# 
#arr_time = arr_df.select('vehicle_time')
#split_col = pyspark.sql.functions.split(arr_time['vehicle_time'], 's ')
#arr_time = arr_time.withColumn('trimtime', split_col.getItem(1))
#arr_time = arr_time.withColumn('dt',to_timestamp(col('trimtime'),"HH:mm:ss"))
#arr_time = arr_time.select('dt')
#arr_time = arr_time.withColumn('hours',hour(col('dt')))
#arr_time = arr_time.withColumn('minutes',minute(col('dt')))
#arr_time = arr_time.withColumn('seconds',second(col('dt')))
#arr_time = arr_time.withColumn('elapsed', col('hours')*60 + col('minutes') + col('seconds')/60)
#arr_time = arr_time.select('elapsed')
#
#listout = arr_time.toPandas().values.tolist()
#l2 = []
#for l in listout:
#    l2.append(l[0])
#    
def vehicle_charts(sens):
    #########NEEDS MINLIST AS AN INPUT#############
    df = spark.read.options(header='True').options(inferschema = 'True').csv('vreport_'+sens+'.csv')
    
    #df2 = spark.read.options(header='True').options(inferschema = 'True').csv('aggreportvehiclesensitivities_v17500.csv')\
    #            .select('VMT','Minute')
    df3 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+sens+'.csv')\
                .filter(col('type') == lit('Dropped'))\
                .withColumn('minute', minute(col('hail_time'))+hour(col('hail_time'))*60)\
                .select('minute')\
                .groupBy('minute').agg(count(col('minute')).alias('dropped_vehicles'))\
                .orderBy('minute')\
                .withColumnRenamed('minute','m')
    #df3.show()
    df = df.join(df3\
                 ,df3.m == df.minute\
                 ,how='inner')\
                 .drop('m','minute')
    
    #df.show()
    df.toPandas().plot()
    df.select('vehicles reallocating').toPandas().plot()
    #df2.toPandas().plot()
    #df3.toPandas().plot()
    
    
##vehicle_charts('vehiclesensitivities_v7500')

def dropped_vehicle_chart():
#    minutelist = []
#    for m in range(0,1440):
#        minutelist.append(m)
#    df = spark.createDataFrame(minutelist, IntegerType())
#    df = df.withColumnRenamed('value','m')
    
    hourlist = []
    for h in range(0,24):
        hourlist.append(h)
    df = spark.createDataFrame(hourlist, IntegerType())
    df = df.withColumnRenamed('value','h')
#    df = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_vehiclesensitivities_v7500.csv')\
#        .filter(col('type') == lit('Dropped'))\
#        .withColumn('minute', minute(col('hail_time'))+hour(col('hail_time'))*60)\
#        .select('minute')\
#        .groupBy('minute').agg(count(col('minute')).alias('dropped_vehicles_7500'))\
#        .orderBy('minute')\
#        .withColumnRenamed('minute','m')
    for vc in [6000,7500,10000,12500,15000]:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_vehiclesensitivities_v'+str(vc)+'.csv')\
            .filter(col('type') == lit('Dropped'))\
            .withColumn('hour', hour(col('hail_time')))\
            .select('hour')\
            .groupBy('hour').agg(count(col('hour')).alias('dropped_rides_'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.ylabel('# Dropped Rides per Hour')
    plt.xlabel('hour of the day')
    return df_p

def VMT_chart():
#    minutelist = []
#    for m in range(0,1440):
#        minutelist.append(m)
#    df = spark.createDataFrame(minutelist, IntegerType())
#    df = df.withColumnRenamed('value','m')
    
    hourlist = []
    for h in range(0,24):
        hourlist.append(h)
    df = spark.createDataFrame(hourlist, IntegerType())
    df = df.withColumnRenamed('value','h')
#    df = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_vehiclesensitivities_v7500.csv')\
#        .filter(col('type') == lit('Dropped'))\
#        .withColumn('minute', minute(col('hail_time'))+hour(col('hail_time'))*60)\
#        .select('minute')\
#        .groupBy('minute').agg(count(col('minute')).alias('dropped_vehicles_7500'))\
#        .orderBy('minute')\
#        .withColumnRenamed('minute','m')
    for vc in [6000,7500,10000,12500,15000]:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_vehiclesensitivities_v'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMTa',col('VMT')/5280)\
            .drop('VMT')\
            .select('hour', 'VMTa')\
            .groupBy('hour').agg(F.avg(col('VMTa')).alias('TotalVMT'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.ylabel('Average VMT')
    plt.xlabel('hour of the day')
    return df_p
#sens = vehiclesensitivities_v10000
#df3 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+sens+'.csv')\
#            .filter(col('type') == lit('Dropped'))\
#            .withColumn('minute', minute(col('hail_time'))+hour(col('hail_time'))*60)\
#            .select('minute')\
#            .groupBy('minute').agg(count(col('minute')).alias('dropped_vehicles'))\
#            .orderBy('minute')\
#            .withColumnRenamed('minute','m')