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
import numpy as np
from pyspark.sql.types import IntegerType, LongType

spark = SparkSession.builder.getOrCreate()

def vehicle_charts(sens):
    
    numv = int(sens.split('_')[3][1:])
    minutelist = []
    for m in range(0,1440):
        minutelist.append(m)
    df = spark.createDataFrame(minutelist, IntegerType())
    df = df.withColumnRenamed('value','m')
    #########NEEDS MINLIST AS AN INPUT#############
    df3 = spark.read.options(header='True').options(inferschema = 'True').csv('vreport_'+sens+'.csv')\
        .withColumn('Idle Vehicles at PUDOs',(col('vehicles_at_PUDOs'))/numv)\
        .withColumn('Enroute non-rideshare',(col('vehicles_enroute')-col('num_rideshares'))/numv)\
        .withColumn('Ridesharing Vehicles',col('num_rideshares')/numv)\
        .withColumn('Reallocating Vehicles',col('vehicles reallocating')/numv)\
        .drop('vehicles_at_PUDOs')\
        .drop('vehicles_enroute')\
        .drop('vehicles reallocating')\
        .drop('num_rideshares')
    
    #df2 = spark.read.options(header='True').options(inferschema = 'True').csv('aggreportvehiclesensitivities_v17500.csv')\
    #            .select('VMT','Minute')
    # df3 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+sens+'.csv')\
    #             .filter(col('type') == lit('Dropped'))\
    #             .withColumn('minute', minute(col('hail_time'))+hour(col('hail_time'))*60)\
    #             .select('minute')\
    #             .groupBy('minute').agg(count(col('minute')).alias('dropped_vehicles'))\
    #             .orderBy('minute')\
    #             .withColumnRenamed('minute','m')
    #df3.show()
    df = df.join(df3\
                 ,df3.minute == df.m\
                 ,how='inner')\
                 .drop('m','minute')
    
    #df.show()
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('% total vehicles', fontsize=15)
    plt.xlabel('minute', fontsize = 15)
    plt.legend(fontsize=15)
    plt.title('Fleet Operations - n='+ str(numv), fontsize = 20)
    #df.select('vehicles reallocating').toPandas().plot()
    #df2.toPandas().plot()
    #df3.toPandas().plot()
    
def vehicle_charts_hour(sens):
    #########NEEDS MINLIST AS AN INPUT#############
    df = spark.read.options(header='True').options(inferschema = 'True').csv('vreport_'+sens+'.csv')
    df = df.withColumn('hour',F.round(col('minute')/60,0))\
            .drop('minute')\
            .groupBy('hour').avg()
    #df2 = spark.read.options(header='True').options(inferschema = 'True').csv('aggreportvehiclesensitivities_v17500.csv')\
    #            .select('VMT','Minute')
    df3 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+sens+'.csv')\
                .filter(col('type') == lit('Dropped'))\
                .withColumn('hour', hour(col('hail_time')))\
                .select('hour')\
                .groupBy('hour').agg(count(col('hour')).alias('dropped_vehicles'))\
                .orderBy('hour')\
                .withColumnRenamed('hour','h')
    #df3.show()
    df = df.join(df3\
                 ,df3.h == df.hour\
                 ,how='inner')\
                 .drop('h','avg(hour)')
    
    #df.show()
    df_p = df.toPandas()
    df_p.plot()
    #df.select('vehicles reallocating').toPandas().plot()
    #df2.toPandas().plot()
    #df3.toPandas().plot()
    return df_p

def ridesharing_chart(senslist):
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
    if senslist == '':
        senslist = ['vehiclesensitivities_v5000'\
                    ,'vehiclesensitivities_v6000'\
                    ,'vehiclesensitivities_v7500'\
                    ,'vehiclesensitivities_v10000'\
                    ,'vehiclesensitivities_v12500'\
                    ,'vehiclesensitivities_v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter(col('type') == lit('Rideshare'))\
            .withColumn('hour', hour(col('hail_time')))\
            .select('hour')\
            .groupBy('hour').agg(count(col('hour')).alias('num_rideshares_'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    #df.show()
    df_p = df.toPandas()
    df_p.plot()
    #plt.figsize(5,5)
    plt.ylabel('# Rideshares per Hour')
    plt.xlabel('hour of the day')
    return df_p
    
    
##vehicle_charts('vehiclesensitivities_v7500')

def dropped_vehicle_chart(senslist):
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter(col('type') == lit('Dropped'))\
            .withColumn('hour', hour(col('hail_time')))\
            .select('hour')\
            .groupBy('hour').agg(count(col('hour')).alias('dropped_rides_'+str(vc)))
        df2 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Dropped'))|(col('type') == lit('Ride')))\
            .withColumn('hour', hour(col('hail_time')))\
            .select('hour')\
            .groupBy('hour').agg(count(col('hour')).alias('ttl_rides_'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
        df = df.join(df2\
                     ,df2.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
        df = df.withColumn('pct_dropped_'+ str(vc), col('dropped_rides_'+str(vc))/col('ttl_rides_'+str(vc)))\
            .drop('dropped_rides_'+str(vc))\
            .drop('ttl_rides_'+str(vc))    
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('% Dropped Rides per Hour', fontsize=15)
    plt.xlabel('hour of the day', fontsize = 15)
    plt.legend(fontsize=15)
    return df_p


def dropped_vehicle_ttl(senslist):
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        namestring = vc.split('_')[-1:][0]
        print(namestring)
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter(col('type') == lit('Dropped'))\
            .withColumn('hour', hour(col('hail_time')))\
            .select('hour')\
            .groupBy('hour').agg(count(col('hour')).alias('dropped_rides_'+str(vc)))

        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')

        df = df.withColumn('ttl_dropped_'+ namestring, col('dropped_rides_'+str(vc)))\
            .drop('dropped_rides_'+str(vc))  
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('Total Dropped Rides per Hour', fontsize=15)
    plt.xlabel('hour of the day', fontsize = 15)
    plt.legend(fontsize=15)
    return df_p

def VMT_chart(senslist):
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare'))|(col('type') == lit('Reallocation')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMTa',(col('VMT')-col('shared_VMT')/2)/5280)\
            .drop('VMT')\
            .select('hour', 'VMTa')\
            .groupBy('hour').agg(F.sum(col('VMTa')).alias('TotalVMT'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    for ccol in df.columns:
        if 'kmean' in ccol:
            df = df.withColumnRenamed(ccol, 'Total VMT K-Means')
            df = df.withColumnRenamed('TotalVMTcsens7500_1750_v7500','Total VMT AFC')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('Total VMT',fontsize=15)
    plt.xlabel('hour of the day',fontsize=15)
    plt.legend(fontsize=15)
    return df_p

def peak_VMT_chart(senslist):
    hourlist = []
    for h in [7,8,16,17,18,19]:
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare'))|(col('type') == lit('Reallocation')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMTa',(col('VMT')-col('shared_VMT')/2)/5280)\
            .drop('VMT')\
            .select('hour', 'VMTa')\
            .groupBy('hour').agg(F.sum(col('VMTa')).alias('TotalVMT'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'inner')
    df = df.orderBy('h').drop('h')
    for ccol in df.columns:
        if 'kmean' in ccol:
            df = df.withColumnRenamed(ccol, 'Total VMT K-Means')
            df = df.withColumnRenamed('TotalVMTcsens7500_1750_v7500','Total VMT AFC')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('Total VMT',fontsize=15)
    plt.xlabel('hour of the day',fontsize=15)
    plt.legend(fontsize=15)
    return df_p

def dropped_VMT_chart(senslist):
    
    hourlist = []
    for h in range(0,24):
        hourlist.append(h)
    df = spark.createDataFrame(hourlist, IntegerType())
    df = df.withColumnRenamed('value','h')
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Dropped')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMTa',(col('VMT')-col('shared_VMT')/2)/5280)\
            .drop('VMT')\
            .select('hour', 'VMTa')\
            .groupBy('hour').agg(F.sum(col('VMTa')).alias('TotalDroppedVMT'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('Total Dropped VMT',fontsize=15)
    plt.xlabel('hour of the day',fontsize=15)
    plt.legend(fontsize=15)
    return df_p

def shared_VMT_chart(senslist):
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare'))|(col('type') == lit('Reallocation')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMTa',(col('shared_VMT')/2)/5280)\
            .drop('VMT')\
            .select('hour', 'VMTa')\
            .groupBy('hour').agg(F.sum(col('VMTa')).alias('SharedVMT'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('Total Shared VMT',fontsize=15)
    plt.xlabel('hour of the day',fontsize=15)
    plt.legend(fontsize=15)
    return df_p


def empty_VMT_chart(senslist):
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMTa',col('empty_VMT')/5280)\
            .drop('VMT')\
            .select('hour', 'VMTa')\
            .groupBy('hour').agg(F.sum(col('VMTa')).alias('Empty_VMT_'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.ylabel('Total Empty VMT')
    plt.xlabel('hour of the day')
    return df_p

def ride_time_chart(senslist):
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('ht_ts',F.to_timestamp(col('hail_time')))\
            .withColumn('hour', hour(col('ht_ts')))\
            .withColumn('at_ts',F.to_timestamp(col('arrival_time')))\
            .withColumn('ride_time',(col("at_ts").cast("long")/60)-(col('ht_ts').cast("long"))/60)\
            .select('hour', 'ride_time')\
            .groupBy('hour').agg(F.avg(col('ride_time')).alias('AverageRideTime'+str(vc)))
        df = df.join(dfj\
                     ,dfj.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.ylabel('Average Ride Time (minutes)',fontsize = 15)
    plt.xlabel('hour of the day',fontsize = 15)
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.legend(fontsize=15)
    return df_p

def detour_time(senslist):
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
    for vc in senslist:
        clusct = vc.split("_")[1]
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('at_ts',F.to_timestamp(col('arrival_time')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('ride_time1',(col("at_ts").cast("long")/60))\
            .filter(col('shared_VMT') > lit(0))\
            .select('hour', 'id', 'ride_time1')
        #dfj.show(1)
        df2 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Ride')))\
            .withColumn('at_ts',F.to_timestamp(col('arrival_time')))\
            .withColumn('ride_time2',(col("at_ts").cast("long")/60))\
            .select('id', 'ride_time2')\
            .withColumnRenamed('id','idd')
        #df2.show(1)
        df2 = df2.join(dfj\
                  ,dfj.id == df2.idd\
                  ,how = 'inner')\
            .withColumn('detour', (col('ride_time1')-col('ride_time2')))\
            .drop('ride_time2','ride_time1','idd','id')\
            .groupBy('hour').agg(F.avg(col('detour')).alias('DetourTime '+clusct+" Clusters"))
        #df2.show(1)    
        df = df.join(df2\
                     ,df2.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.ylabel('Avg Detour Time (minutes)',fontsize=15)
    plt.xlabel('hour of the day', fontsize=15)
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.legend(fontsize=15)
    return df_p

def detour_VMT(senslist, sumavg):
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        clusct = vc.split("_")[1]
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMT1',(col('VMT'))/5280)\
            .filter(col('shared_VMT') > lit(0))\
            .select('hour', 'id', 'VMT1')
            
        df2 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Ride')))\
            .withColumn('VMT2',col('VMT')/5280)\
            .select('id', 'VMT2')\
            .withColumnRenamed('id','idd')

        df2 = df2.join(dfj\
                  ,dfj.id == df2.idd\
                  ,how = 'inner')\
            .withColumn('detour', (col('VMT1')-col('VMT2')))\
            .drop('VMT2','VMT1','idd','id')\
            .filter(col('detour') > lit(0))
        if sumavg == 'sum':
            df2 = df2.groupBy('hour').agg(F.sum(col('detour')).alias('DetourVMT '+clusct+" Clusters"))
        if sumavg == 'avg':
            df2 = df2.groupBy('hour').agg(F.avg(col('detour')).alias('DetourVMT '+clusct+" Clusters"))
             
        df = df.join(df2\
                     ,df2.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')

    #df.show()
    df_p = df.toPandas()
    for coll in df_p.columns:
        df_p[coll] = df_p[coll].fillna(0)
    df_p.plot()
    if sumavg == 'sum':
        plt.ylabel('Total Detour VMT (miles)', fontsize=15)
    if sumavg == 'avg':
        plt.ylabel('Average Detour VMT (miles)', fontsize=15)
    plt.xlabel('hour of the day', fontsize=15)
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.legend(fontsize=15)
    return df_p

def detour_total(senslist):

#    df = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_vehiclesensitivities_v7500.csv')\
#        .filter(col('type') == lit('Dropped'))\
#        .withColumn('minute', minute(col('hail_time'))+hour(col('hail_time'))*60)\
#        .select('minute')\
#        .groupBy('minute').agg(count(col('minute')).alias('dropped_vehicles_7500'))\
#        .orderBy('minute')\
#        .withColumnRenamed('minute','m')
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('ride_time1',(col("arrival_time").cast("long")/60))\
            .select('id', 'ride_time1')
            
        df2 = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Ride')))\
            .withColumn('ride_time2',(col("arrival_time").cast("long")/60))\
            .select('id', 'ride_time2')\
            .withColumnRenamed('id','idd')
        if 'vehicle' in vc:
            vc = 'async2000_v10000'
        df2 = df2.join(dfj\
                  ,dfj.id == df2.idd\
                  ,how = 'inner')\
            .withColumn('detour', (col('ride_time1')-col('ride_time2')))\
            .drop('ride_time2','ride_time1','idd','id')\
            .filter(col('detour') > lit(0))\
            .groupBy().agg(F.avg(col('detour')).alias('AverageDetour'+str(vc)))
        df_p = df2.toPandas()
        print(df_p.head(1))
    return df_p

def uc_walk(senslist):
#    minutelist = []
#    for m in range(0,1440):
#        minutelist.append(m)
#    df = spark.createDataFrame(minutelist, IntegerType())
#    df = df.withColumnRenamed('value','m')
    df_uc = spark.read.options(header='True').options(inferschema = 'True').csv('urban_core_nodes.csv')\
        .select("id")
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        if "centroid" in vc:
            lbl = 'Congestion Range PUDOs'
        else:
            lbl = 'Centroid PUDOs'
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('walktime', col("total_walk")/lit(4.4)/lit(60))\
            .select('hour', "walktime",'origin','destination')
        #dfj.filter(col('dPUDO') != col('destination')).show()
        #print(dfj.dtypes)
        df1 = dfj.join(df_uc\
                       ,dfj.origin == df_uc.id\
                       ,how = 'inner')\
                       .drop('id')
        df2 = dfj.join(df_uc\
                       ,dfj.destination == df_uc.id\
                       ,how = 'inner')\
                       .drop('id')
        df1 = df1.union(df2)
        df1 = df1.select('hour','walktime')\
                .groupBy('hour').agg(F.sum(col('walktime')).alias('Avg Walk Time in Urban Core'+lbl))

        df = df.join(df1\
                     ,df1.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.ylabel('Average Walk Time (minutes)', fontsize=15)
    plt.xlabel('hour of the day', fontsize=15)
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.legend(fontsize=15)
    return df_p

def uc_vmt(senslist):
#    minutelist = []
#    for m in range(0,1440):
#        minutelist.append(m)
#    df = spark.createDataFrame(minutelist, IntegerType())
#    df = df.withColumnRenamed('value','m')
    df_uc = spark.read.options(header='True').options(inferschema = 'True').csv('urban_core_nodes.csv')\
        .select("id")
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        if "centroid" in vc:
            lbl = 'Congestion Range PUDOs'
        else:
            lbl = 'Centroid PUDOs'
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('hour', hour(col('hail_time')))\
            .withColumn('VMTa',(col('VMT')-col('shared_VMT')/2)/5280)\
            .drop('VMT')\
            .select('hour', 'VMTa','origin','destination')

        #dfj.filter(col('dPUDO') != col('destination')).show()
        #print(dfj.dtypes)
        df1 = dfj.join(df_uc\
                       ,dfj.origin == df_uc.id\
                       ,how = 'inner')\
                       .drop('id')
        df2 = dfj.join(df_uc\
                       ,dfj.destination == df_uc.id\
                       ,how = 'inner')\
                       .drop('id')
        df1 = df1.union(df2)
        df1 = df1.select('hour','VMTa')\
                .groupBy('hour').agg(F.sum(col('VMTa')).alias('Total VMT - Urban Core Trips'+lbl))

        df = df.join(df1\
                     ,df1.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('Total VMT',fontsize=15)
    plt.xlabel('hour of the day',fontsize=15)
    plt.legend(fontsize=15)
    return df_p

def uc_ridetime(senslist):
#    minutelist = []
#    for m in range(0,1440):
#        minutelist.append(m)
#    df = spark.createDataFrame(minutelist, IntegerType())
#    df = df.withColumnRenamed('value','m')
    df_uc = spark.read.options(header='True').options(inferschema = 'True').csv('urban_core_nodes.csv')\
        .select("id")
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
    if senslist == '':
        senslist = ['v5000','v6000','v7500','v10000','v12500','v15000']
    for vc in senslist:
        if "centroid" in vc:
            lbl = 'Congestion Range PUDOs'
        else:
            lbl = 'Centroid PUDOs'
        dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+str(vc)+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .withColumn('ht_ts',F.to_timestamp(col('hail_time')))\
            .withColumn('hour', hour(col('ht_ts')))\
            .withColumn('at_ts',F.to_timestamp(col('arrival_time')))\
            .withColumn('ride_time',(col("at_ts").cast("long")/60)-(col('ht_ts').cast("long"))/60)\
            .withColumn('walk_time',col("total_walk")/lit(4.4)/lit(60))\
            .withColumn('total_travel_time',col('ride_time') + col('walk_time'))\
            .select('hour', 'total_travel_time', 'origin','destination')

        #print(dfj.show())
        #dfj.filter(col('dPUDO') != col('destination')).show()
        #print(dfj.dtypes)
        df1 = dfj.join(df_uc\
                       ,dfj.origin == df_uc.id\
                       ,how = 'inner')\
                       .drop('id')
        df2 = dfj.join(df_uc\
                       ,dfj.destination == df_uc.id\
                       ,how = 'inner')\
                       .drop('id')
        df1 = df1.union(df2)
        df1 = df1.select('hour','total_travel_time')\
                .groupBy('hour').agg(F.sum(col('total_travel_time')).alias('Total Travel Time - Urban Core Trips'+lbl))

        df = df.join(df1\
                     ,df1.hour == df.h\
                     ,how = 'outer')\
                     .drop('hour')
    df = df.orderBy('h').drop('h')
    df_p = df.toPandas()
    df_p.plot()
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.ylabel('Total Travel Time (minutes)',fontsize=15)
    plt.xlabel('hour of the day',fontsize=15)
    plt.legend(fontsize=15)
    return df_p

def histogram(sens, strvar):
    pass

#%matplotlib inline
plt.rcParams["figure.figsize"] = [15, 10]
#df.where(col('col1').like("%string%")).show()


# def reallocation_by_node(sens):
#     df_n = spark.read.options(header='True').options(inferschema = 'True').csv(os.path.dirname(os.getcwd))+'\\nodeDict.csv')
#     dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+sens+'.csv')\
#     .where(col('ID').like("%reloc%"))\
#     .select('destination')

def walk_ride_scatter(sens):
    dfj = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+sens[0]+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .filter(col('total_walk') > lit(0))\
            .withColumn('ht_ts',F.to_timestamp(col('hail_time')))\
            .withColumn('hour', hour(col('ht_ts')))\
            .withColumn('at_ts',F.to_timestamp(col('arrival_time')))\
            .withColumn('ride_time1',(col("at_ts").cast("long")/60)-(col('ht_ts').cast("long"))/60)\
            .withColumn('walk_time1',col("total_walk")/lit(4.4)/lit(60))\
            .select('id','walk_time1','ride_time1')
    dfk = spark.read.options(header='True').options(inferschema = 'True').csv('reporting_'+sens[1]+'.csv')\
            .filter((col('type') == lit('Arrival'))|(col('type') == lit('Rideshare')))\
            .filter(col('total_walk') > lit(0))\
            .withColumn('ht_ts',F.to_timestamp(col('hail_time')))\
            .withColumn('hour', hour(col('ht_ts')))\
            .withColumn('at_ts',F.to_timestamp(col('arrival_time')))\
            .withColumn('ride_time2',(col("at_ts").cast("long")/60)-(col('ht_ts').cast("long"))/60)\
            .withColumn('walk_time2',col("total_walk")/lit(4.4)/lit(60))\
            .select('id','walk_time2','ride_time2')\
            .withColumnRenamed('id','idd')
    df = dfj.join(dfk\
                  ,dfj.id == dfk.idd\
                  ,how = 'inner')\
                    .drop('idd')
    df = df.withColumn('walk_time_delta',col('ride_time1')-col('ride_time2'))\
            .withColumn('ride_time_delta',col('walk_time2')-col('walk_time1'))\
            .drop('walk_time1','walk_time2','ride_time1','ride_time2','id')
    df_p = df.toPandas()
    df_p = df_p[['walk_time_delta','ride_time_delta']]
    x = df_p['walk_time_delta']
    y = df_p['ride_time_delta']
    plt.scatter(x,y)
    z = np.polyfit(x,y, 1)
    print(z)
    p = np.poly1d(z)
    plt.plot(x,p(x),"r--")
    plt.xlabel('walk_time_delta',fontsize=15)
    plt.ylabel('ride_time_delta',fontsize=15)
    plt.show()
