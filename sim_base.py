# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import numpy
import pandas
import math
from ast import literal_eval

zones_df = pandas.read_csv(r'c:\users\duncan.anderson\documents\sim_zones.csv')#,index_col = "ID")
stations = []
#num_drones = 1000
drone_speed = .7
drone_efficiency = .5
station_cap = 500
maxdeliver = 1000
charge_rate = 2
replications = 100
reports = {}
#wander = False
#Optimal = False ##Optimizes wander by returning the drone to the station with the most demand in the next cycle
#Weighted = False
#run_id = '10x10-5stat-1000del-100rep'

#'
   
#else:
#    events_df = pandas.DataFrame()
#    permute_id = ''
    
class Vehicle(object):
    def __init__(self,home, location, charge_level):
        self.home = home
        self.charge_level = charge_level
    def get_charge_level(self):
        return self.charge_level
    def get_location(self):
        return self.location
    def get_home(self):
        return self.home

class Charge_Station(object):
    def __init__(self, ID, loc, capacity, max_cap):
        self.ID = ID
        self.loc = loc
        self.capacity = capacity
        self.max_cap = max_cap
        self.adjacencies = 0
        self.demand = {}
    def get_id(self):
        return self.ID
    def get_location(self):
        return self.loc
    def get_capacity(self):
        return self.capacity
    def get_max_cap(self):
        return self.max_cap
    def get_distance(self):
        return self.adjacencies
    def get_adjacencies(self):
        return self.adjacencies
    def get_demand(self):
        return self.demand
    
class Zone(object):
    def __init__(self, ID, location, p_orig, lambda_dest):
        self.ID = ID
        self.location = location
        self.p_orig = p_orig
        self.lambda_dest = lambda_dest
        self.next_deliver = 0
        self.station = Charge_Station(0,self.location,[],station_cap)
        self.completed_deliveries = []
    def get_lambda(self):
        return self.lambda_dest
    def get_location(self):
        return self.location
    def get_ID(self):
        return self.ID
    def get_p_orig(self):
        return self.p_orig
    def get_next_deliver(self):
        return self.next_deliver
    def get_completed_deliveries(self):
        return self.completed_deliveries
    def get_station(self):
        return self.station

    
class Delivery(object):
    def __init__(self, order_time, origin, destination, pickup_time, dropoff_time):
        self.order_time = order_time
        self.origin = origin
        self.destination = destination
        self.drone_servicing = 0
        self.pickup_time = pickup_time
        self.dropoff_time = dropoff_time
        self.delivery_time = 0
        self.return_time = 0
        self.queue_time = 0
    def get_origin(self):
        return self.origin
    def get_order_time(self):
        return self.order_time
    def get_destination(self):
        return self.destination
    def get_pickup_time(self):
        return self.pickup_time
    def get_dropoff_time(self):
        return self.dropoff_time
    def get_drone_servicing(self):
        return self.drone_servicing
    def get_delivery_time(self):
        return self.delivery_time
    def get_return_time(self):
        return self.return_time
    def get_queue_time(self):
        return self.queue_time

def run_sim(run_id, permute, permute_id, eventlog_id,num_drones):
    if eventlog_id == '':
        events_df = pandas.DataFrame()
    else:
        events_df = pandas.read_excel(r'C:\Users\duncan.anderson\Documents\reporting'+eventlog_id+r'.xlsx')
    report_data = initialize(run_id, permute, permute_id, eventlog_id, events_df, num_drones)
    report_data.to_excel(r'c:\users\duncan.anderson\documents\REPORTING\reporting'+run_id+permute_id+'.xlsx')
    return report_data

def initialize(run_id, permute, permute_id, eventlog_id, events_df,num_vehicles, PUDO_list):
    print('initializing')
    time =0
    stations = []
    #create the stations
        #function that loads the PUDO csv into station objects
    
    #create the vehicles
    

    for i in range(replications):
        #this conditional only required for stochastic replications
        
        
        schedule = []
        if permute is False:
            print('no existing schedule')
            schedule = create_schedule(None)
        else:
            schedule = create_schedule(events_df.loc[events_df['replication'] == i])
        stations = reset(stations,schedule,num_drones,i)
        time = 0
        dlog = {}
        print('running replication',i, 'of permutation', run_id+permute_id)
        while len(dlog)<maxdeliver:
            (time, schedule, dlog) = sim_step(schedule, stations, time, dlog, wander,Optimal,permute)
        if i == 0:
            report_data = reporting(i, dlog, None,run_id+permute_id)
        else:
            report_data = reporting(i, dlog, report_data,run_id+permute_id)
    return report_data
    
def reset(stations, schedule,num_drones,i):
    ###########WIPE DELIVERY RECORDS AND NEXT DELIVERIES FROM EACH ZONE############
    for i in zones_df.obj:
        i.completed_deliveries = []
        i.next_deliver = 0
    for s in stations:
        s.adjacencies = 0
        s.capacity = []
        
    ###########INITIALIZE DRONES###############

        #where N is the # of drones in the system, allocates drones according to the first N deliveries distribution
        #then for each station calculates the appropriate proportion of drones to home there for the next N timesteps
    for d in schedule.keys():
        for s in stations:
            s.demand[d] = 0
        sched15 = {}
        for d2 in schedule.keys():
            if d2 < 15+d and d2+10>d:
                sched15[d2] = schedule[d2]
        for d3 in sched15.values():
            for s in stations:
                if calc_best_station(d3, stations, wander, Optimal) == s:
                #if calc_nearest_station(stations, d3.get_origin().get_location()) == s:
                    #if d == min(schedule.keys()):
                        #print('increasing demand at',s.get_location(),'by 1 at time',d,'total is now',s.get_demand()[d]+1)
                    s.demand[d] += 1
        if d == min(schedule.keys()):
            for s in stations:
                print('station @', s.get_location(), 'gets # drones', int(num_drones* s.get_demand()[d]/len(sched15)))
                for dc in range(int(num_drones* s.get_demand()[d]/len(sched15))):
                    s.capacity.append(Drone(s,100))
            
    return stations
    
def calc_nearest_station(stations,coord):
    #for a set of coordinates (coord) returns the closest PUDO
    closest = {}
    d_x = coord[0]
    d_y = coord[1]
    for s in stations:
        s_x = s.get_location()[0]
        s_y = s.get_location()[1]
        closest[s] = numpy.sqrt((s_x-d_x)**2+(s_y-d_y)**2)
    return min(closest, key=closest.get)

def calc_best_station(delivery, stations, wander, Optimal):
    travel_distance = {}
    return_distance = {}
    o_x = delivery.get_origin().get_location()[0]#-numpy.random.uniform()
    o_y = delivery.get_origin().get_location()[1]#-numpy.random.uniform()
    d_x = delivery.get_destination().get_location()[0]#-numpy.random.uniform()
    d_y = delivery.get_destination().get_location()[1]#-numpy.random.uniform()
    for i in stations:

        s_x = i.get_location()[0]
        s_y = i.get_location()[1]
        travel_distance[i] = (numpy.sqrt((abs(s_x-o_x)**2+abs(s_y-o_y)**2)))
        travel_distance[i] += numpy.sqrt((abs(o_x-d_x)**2+abs(o_y-d_y)**2))
    chosen_station = min(travel_distance, key=travel_distance.get)
    #print('for delivery',delivery.get_origin().get_location(), delivery.get_destination().get_location(),'optimal station is',chosen_station.get_location())
    return chosen_station

def assign_origin():
    x=0
    Uni = numpy.random.uniform()
    t=-1
    while x <= Uni:
        t+=1
        x += zones_df.loc[t,'p_orig']
    return zones_df.loc[t,'obj']

def create_schedule(events_df):
    
    ###LOOKS FOR AN EXISTING SCHEDULE OF EVENTS
    schedule = {}

    return schedule

def gen_delivery(destination, time, schedule,setval):
    
    ###TURNS A SCHEDULED EVENT INTO A DELIVERY######
    
    #print(d.get_ID(),d.get_lambda())
    if setval is None:
        #print(destination.get_lambda())
        new_deliver = Delivery(time+numpy.random.exponential(destination.get_lambda()),assign_origin(),destination, numpy.random.normal(1,.25),numpy.random.normal(2,.25))
    ###print('generating a new delivery at: t/o/d', new_deliver.get_order_time(),new_deliver.get_origin().get_location(),new_deliver.get_destination().get_location())
    else:
        new_deliver = Delivery(setval[0],setval[1],destination,setval[2],setval[3])
    destination.next_deliver = new_deliver
    schedule[new_deliver.get_order_time()] = new_deliver
    return schedule        

def sim_step(schedule, stations, oldtime, dlog,wander,Optimal,permute):
    ####EVENT DRIVEN SIMULATION SO THE CURRENT TIME IS THE MINIMUM TIME OBJECT IN THE SCHEDULE####
    time = min(schedule.keys())
    delta_t = time - oldtime
    charge_drones(stations,delta_t)
    event = schedule.pop(time)
    if type(event) == Delivery:
        if permute == False:
            schedule = gen_delivery(event.get_destination(), time, schedule,None)
        event = dispatch(event, stations,wander,Optimal)
        #if no drone is available anywhere, the delivery is punted until after the next one pops
        if event.get_drone_servicing() == 0:
            next_drone_arr = min(x for x in schedule.keys() if type(schedule[x])==Drone)
            U4key = numpy.random.uniform()
            schedule[next_drone_arr+U4key] = event
            ###print('punting delivery',event.get_order_time(),'until drone returns at',next_drone_arr)
        else:
            #the delivery is unobstructed and will proceed normally
            ###print('drone will return @',event.get_total_time()+time)
            event.queue_time = time-event.get_order_time()
            event.delivery_time += event.get_queue_time()
            schedule[event.get_delivery_time()+event.get_return_time()+time] = event.get_drone_servicing()
            dlog[event.get_order_time()] = event
    #drone is inflight and its arrival is treated as an event
    elif type(event) == Drone:
        ###print('drone ', event.get_ID(), 'returning home to', event.get_home().get_location())
        event.home.capacity.append(event)
    return (time, schedule, dlog)
    
def charge_drones(stations,delta_t):
    for s in stations:
        for d in s.get_capacity():
            d.charge_level = min(d.charge_level+(charge_rate*delta_t),100)
                
def dispatch(delivery, stations):
    #print('dispatching a drone')
    travel_distance = {}
    return_distance = {}
    o_x = delivery.get_origin().get_location()[0]#-numpy.random.uniform()
    o_y = delivery.get_origin().get_location()[1]#-numpy.random.uniform()
    d_x = delivery.get_destination().get_location()[0]#-numpy.random.uniform()
    d_y = delivery.get_destination().get_location()[1]#-numpy.random.uniform()
    for i in stations:
        #sort the drone capacity in each station by charge level
        i.capacity.sort(key=lambda x: x.get_charge_level(),reverse=True)
        #for stations with capacity, calculate the total travel distance for the route
        if len(i.get_capacity())==0:
            #print('charge station has no drones @',i.get_location())
            travel_distance[i] = 9999
        else:
            s_x = i.get_location()[0]
            s_y = i.get_location()[1]
            if wander==True:
                newhome = calc_nearest_station(stations,delivery.get_destination().get_location())
                (f_x,f_y) = (newhome.get_location()[0],newhome.get_location()[1])
                #print('drone homed at', i.get_location(), 'rehomed to', newhome.get_location(), 'from dest', delivery.get_destination().get_location())
            elif Optimal==True:
                newhome = calc_optimal_home(stations, delivery.get_order_time())
                (f_x,f_y) = (newhome.get_location()[0],newhome.get_location()[1])
            else:
                (f_x,f_y) = (s_x,s_y)
            travel_distance[i] = (numpy.sqrt((abs(s_x-o_x)**2+abs(s_y-o_y)**2)))
            travel_distance[i] += numpy.sqrt((abs(o_x-d_x)**2+abs(o_y-d_y)**2))
            return_distance[i] = numpy.sqrt((abs(d_x-f_x)**2+abs(d_y-f_y)**2))
    #remove stations where the most charged drone cannot cover the route
    for i in travel_distance.keys():
        (travel_distance[i], i.get_location())
        if len(i.get_capacity())>0:
            if i.get_capacity()[0].get_charge_level() < travel_distance[i]+return_distance[i] / drone_efficiency:
                #print('charge station drones not adequately charged @',i.get_location(), travel_distance[i], delivery.get_order_time())
                travel_distance[i] = 9999

    if min(travel_distance.values()) == 9999:
        #print('first punted delivery',delivery.get_order_time())
        return delivery
    chosen_station = min(travel_distance, key=travel_distance.get)
    delivery_distance = travel_distance[chosen_station]
    
    return_dist = return_distance[chosen_station]
    delivery.return_time = return_dist / drone_speed
    #print('delivery @ o/d:',delivery.get_origin().get_location(), delivery.get_destination().get_location(), 'chooses station:',chosen_station.get_location())
    delivery.delivery_time = delivery.get_pickup_time() + delivery.get_dropoff_time() + (delivery_distance / drone_speed)
   
    #capacity list is sorted by charge so the drone with the most charge is selected
    chosen_drone = chosen_station.get_capacity()[0]
    #drone is assigned to the delivery
    delivery.drone_servicing = chosen_drone
    #drone is removed from the capacity list for the station
    chosen_station.capacity.pop(0)
    #the drone has the appropriate amount of charge deducted
    chosen_drone.charge_level -= delivery.get_delivery_time() + delivery.get_return_time() / drone_efficiency
    if wander==True:
        newhome = calc_nearest_station(stations,delivery.get_destination().get_location())
        chosen_drone.home = newhome
    if Optimal == True:
        newhome = calc_optimal_home(stations, delivery.get_order_time())
        #print('drone rehoming to', newhome.get_location(), 'from destination',delivery.get_destination().get_location())
        chosen_drone.home = newhome
    return delivery

def reporting(rep, dlog, rep_df,run_id):

    report_d = {}
    for d in dlog.values():
        keyval = d.get_order_time()
        report_d[keyval]= [d.get_origin().get_location()]
        report_d[keyval].append(d.get_destination().get_location())
        report_d[keyval].append(d.get_drone_servicing().get_home().get_location())
        report_d[keyval].append(d.get_queue_time())
        report_d[keyval].append(d.get_return_time())
        report_d[keyval].append(d.get_delivery_time())
        report_d[keyval].append(rep)
        report_d[keyval].append(run_id)
    if rep_df is None:
        rep_df = pandas.DataFrame.from_dict(report_d, orient='index',columns=['origin','destination','drone from','queue_time','return_time','delivery_time','replication','runid'])
    else:
        temp_df = pandas.DataFrame.from_dict(report_d, orient='index',columns=['origin','destination','drone from','queue_time','return_time','delivery_time','replication','runid'])
        rep_df = pandas.concat([rep_df,temp_df])
    return(rep_df)

#run_sim(run_id, permute, wander, Optimal, Weighted, permute_id, eventlog_id,num_drones):
def normbound():
    #output_data = run_sim('optimal',True,False,True,False,'200','normal1000',200)
    
    #for i in range(700,500,-100):
        #numdrones = i
        #output_data = run_sim('normal',True,False,False,False,str(i),'normal1000',i)
        
    for i in range(280,290,10):
        #numdrones = i
        #output_data = run_sim('wander',True,True,False,False,str(i),'normal1000',i)
        output_data = run_sim('1station',True,False,False,False,str(i),'normal1000',i)
        #output_data = run_sim('optimal10-15tests',True,False,True,False,str(i),'normal1000',i)
        #output_data = run_sim('weighted',True,False,False,True,str(i),'normal1000',i)
        #output_data = run_sim('weightedwander',True,True,False,True,str(i),'normal1000',i)
        #output_data = run_sim('final',False,False,False,False,'normal1000','10x10-5stat-1000del-100rep')
    return output_data
normbound()
