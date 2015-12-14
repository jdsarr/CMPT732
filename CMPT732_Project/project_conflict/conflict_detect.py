#---------------------------------------------------------------------------------------------------------------------#
#-- CMPT 732 Project - Conflict Dectection on Flight Data ------------------------------------------------------------#
#-- Filename: conflict_detect.py -------------------------------------------------------------------------------------#
#-- By: Juan Sarria --------------------------------------------------------------------------------------------------#
#-- Date: November 28, 2015 ------------------------------------------------------------------------------------------#
#-- Description: -----------------------------------------------------------------------------------------------------#
#-- Main file where the process is executed. Conflict detectors will be called on and the results will be printed   --#
#-- a map ------------------------------------------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------------------------------------#

import sys, os
from math import sqrt
from pyspark import SparkContext, SparkConf, SQLContext, rdd
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
from gmplot import GoogleMapPlotter

DEBUG_MAP = 1
MAIN_COMMANDS = ['-delay', '-prox']
conf = SparkConf().setAppName('Conflict Detection - CMPT732 Project')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


########################################################################################################################
def get_path(line):

    path = '/user/jsarria/flightdata'

    date = line.strip().split('/')
    if len(date) !=3:
        raise Exception('Error: Date format incorrect')

    year  = date[0]
    month = date[1]
    day   = date[2]

    if year == '':
        year = '*'
        path+='/*'
    elif int(year) < 2000 or int(year) > 2005:
        raise Exception('Error: Year out of bounds')
    else:
        path+= ('/' + year)

    if month == '':
        month = '*'
        path+='/*'
    elif int(month) < 1 or int(month) > 12:
        raise Exception('Error: Month out of bounds')
    else:
        if int(month) < 10:
            month = '0' + str(int(month))
        path+= ('/'+year+month+'m')

    if day == '':
        path+='/*'
    elif int(day) < 1 or int(day) > 31:
        raise Exception('Error: Day out of bounds')
    else:
        if year == month == '*':
            year = ''
        if int(day) < 10:
            day = '0' + str(int(day))
        path+= ('/tsmft'+year+month+day+'.bz2')

    return path
########################################################################################################################
def handle_args(args):
    airports = []
    commands = []
    if len(args) > 1:
        for idx, arg in enumerate(args[1:len(args)]):
            print str(idx) + ':' + str(arg)
            if not arg.startswith('-') and idx <= 1:
                airports.append(arg)
            elif (not arg.startswith('-') and idx > 1) or (arg.startswith('-') and arg not in MAIN_COMMANDS):
                print 'Did not process ' + arg
            elif arg.startswith('-'):
                commands.append(arg)
    if len(airports) == 0:
            airports = ['LAX']
    if len(commands) == 0:
            commands = ['-delay']
    return airports, commands
########################################################################################################################
def parse_flights(line):

    #Data parsing determined from http://angler.larc.nasa.gov/flighttracks/proc.html
    flight_data = line.strip().split('|')
    date  = flight_data[0].split('-')
    year  = int(date[0])
    month = int(date[1])
    day   = int(date[2])
    time  = float(flight_data[1])    #UTC hour
    lat   = float(flight_data[2])    #degree
    lon   = float(flight_data[3])    #degree
    dest_lat = float(flight_data[4]) #degree
    dest_lon = float(flight_data[5]) #degree
    alt   = float(flight_data[6])    #degree
    flight_id = flight_data[7]

    dep_airport = flight_data[8]
    arr_airport = flight_data[9]
    dep_time    = float(flight_data[10])  #UTC hour
    eta         = float(flight_data[11])  #UTC hour
    status      = flight_data[12]         #P for pending, E for en route
    speed       = float(flight_data[13])  #knots
    heading     = float(flight_data[14])  #degree
    craft_type  = flight_data[15]

    return {'year': year, 'month':month, 'day':day, 'time':time, 'lat':lat, 'lon':lon,'dest_lat':dest_lat,
            'dest_lon': dest_lon, 'alt': alt, 'flight_id': flight_id, 'dep_airport': dep_airport,
            'arr_airport': arr_airport, 'dep_time': dep_time, 'eta': eta, 'status': status, 'speed': speed,
            'heading': heading, 'craft_type': craft_type}
########################################################################################################################
def parse_airports(line):

    #Data parsing determined from http://openflights.org/data.html
    # 5925,"Al-Jawf Domestic Airport","Al-Jawf","Saudi Arabia","AJF","OESK",29.785133,40.100006,2261,3,"U","Asia/Riyadh"
    airport_data = line.strip().split('",')
    #5925,"Al-Jawf Domestic Airport
    #"Al-Jawf
    #"Saudi Arabia
    #"AJF
    #"OESK
    #29.785133,40.100006,2261,3,"U","Asia/Riyadh"

    a_id    = airport_data[0].split(',"')[0]
    name    = airport_data[0].split(',"')[1]
    city    = airport_data[1].strip('"')
    country = airport_data[2].strip('"')
    FAAcode = airport_data[3].strip('"')
    ICAOcode= airport_data[4].strip('"')
    airport_data = airport_data[5].split(',')

    #29.785133
    #40.100006
    #2261
    #3
    #"U"
    #"Asia/Riyadh"
    lat      = float(airport_data[0])
    lon      = float(airport_data[1])
    alt      = float(airport_data[2])
    timezone = float(airport_data[3])
    dst      = airport_data[4].strip('"') #dst symbols explained in http://openflights.org/help/time.html

    return {'a_id': a_id,'name': name, 'city': city, 'country': country, 'FAA': FAAcode, 'ICAO': ICAOcode,
            'lat': lat, 'lon': lon, 'alt': alt, 'timezone': timezone, 'dst': dst}
########################################################################################################################
def get_airport_data(airports):
    airportdata = open('airports.dat','r').read().split('\n')
    airportdata = sc.parallelize(airportdata).map(parse_airports).filter(lambda x: x['FAA'] in airports
                                                                                   and x['country'] == 'United States')
    airportdata = airportdata.collect()
    if len(airportdata) == 0:
        raise Exception('Error: No airports founds')
    elif len(airports) != len(airportdata):
        raise Exception('Error: At least one airport not found')

    return airportdata
########################################################################################################################
def get_area(airportdata, i):
    lats = []
    lons = []
    for airport in airportdata:
        lats.append(airport['lat']+ i)
        lats.append(airport['lat']- i)
        lons.append(airport['lon']+ i)
        lons.append(airport['lon']- i)

    lat_range = {'min': min(lats), 'max': max(lats)}
    lon_range = {'min': min(lons), 'max': max(lons)}
    return (lat_range, lon_range)
########################################################################################################################
def get_boundary_paths(range_dict):
    boundary_paths = [{'lat_range': [range_dict['lat']['min'],range_dict['lat']['max']],
                       'lon_range': [range_dict['lon']['min'],range_dict['lon']['min']]},
                      {'lat_range': [range_dict['lat']['min'],range_dict['lat']['max']],
                       'lon_range': [range_dict['lon']['max'],range_dict['lon']['max']]},
                      {'lat_range': [range_dict['lat']['min'],range_dict['lat']['min']],
                       'lon_range': [range_dict['lon']['max'],range_dict['lon']['min']]},
                      {'lat_range': [range_dict['lat']['max'],range_dict['lat']['max']],
                       'lon_range': [range_dict['lon']['max'],range_dict['lon']['min']]}]
    return boundary_paths
########################################################################################################################
def detect_delay(alldata, airports,gmap):

    def is_late(time,eta,dep_time):
        if time<=eta:
            return False
        elif dep_time > eta and time >= dep_time and time <= 24:
            return False
        elif time > 24 and time %24 < eta:
            return False
        else:
            return True

    def reduce_key(a,b):
        return a

    def label(time, eta, dep_time):
        if is_late(time,eta,dep_time):
            return 'r'
        else:
            return 'k'

    def get_delay_time(x):
        key = x[0]
        value_list = x[1]

        flight_id = key[0]
        year = key[1]
        month = key[2]
        day = key [3]
        dep_time = key[4]

        delay_time = 0
        craft_type = None

        for val in value_list:
            time = val[0]
            eta = val[3]
            if (time - eta) > delay_time:
                delay_time = time-eta
                craft_type = val[6]

        return (flight_id,year,month,day,dep_time,craft_type,delay_time)

    def get_min_max(a,b,find_max=True):
        delay1 = a[6]
        delay2 = b[6]
        if find_max == False:
            delay1*=-1
            delay2*=-1
        if delay1 > delay2:
            return a
        else:
            return b

    def tuple_sum(a,b):
        return ((a[0]+b[0]),(a[1]+b[1]))



    delayedflights = alldata.filter(lambda x: is_late(x['time'],x['eta'],x['dep_time']))
    delayedflights = delayedflights.map(lambda x: ((x['flight_id'],x['year'],x['month'],x['day'],x['dep_time']),1))
    delayedflights = delayedflights.reduceByKey(reduce_key)
    delayedflights = delayedflights.map(lambda ((fid,year,month,day,dep_time),num): (fid,year,month,day,dep_time))

    allflights = alldata.map(lambda x: (x['flight_id'],x['year'],x['month'],x['day'],x['time'],x['lat'],x['lon'],
                                        x['dep_time'],x['eta'],x['arr_airport'], x['craft_type']))
    alldata.unpersist()

    delay_schema =  StructType([
    StructField('flight_id', StringType(), False),
    StructField('year',IntegerType(),False),
    StructField('month',IntegerType(),False),
    StructField('day',IntegerType(),False),
    StructField('dep_time',FloatType(),False),
    ])

    flight_schema=  StructType([
    StructField('flight_id', StringType(), False),
    StructField('year',IntegerType(),False),
    StructField('month',IntegerType(),False),
    StructField('day',IntegerType(),False),
    StructField('time',FloatType(),False),
    StructField('lat',FloatType(),False),
    StructField('lon',FloatType(),False),
    StructField('dep_time',FloatType(),False),
    StructField('eta',FloatType(),False),
    StructField('arpt',StringType(),False),
    StructField('craft_type',StringType(),False),
    ])

    sqlContext.createDataFrame(delayedflights,schema=delay_schema).registerTempTable('dt')
    sqlContext.createDataFrame(allflights,schema=flight_schema).registerTempTable('ft')

    delay_df = sqlContext.sql("""
        SELECT ft.flight_id AS flight_id, ft.year AS year, ft.month AS month, ft.day AS day, ft.time AS time, ft.lat AS lat, ft.lon AS lon, ft.dep_time AS dep_time, ft.eta AS eta, ft.arpt AS arpt, ft.craft_type AS craft_type
        FROM ft
        INNER JOIN dt
        ON ft.flight_id = dt.flight_id AND ft.year = dt.year AND ft.month = dt.month AND ft.day = dt.day AND ft.dep_time = dt.dep_time
    """)

    delayedflights = delay_df.rdd.map(lambda row: (row,label(row.time,row.eta,row.dep_time)))
    delayedflights = delayedflights.map(lambda (row,label): ((row.flight_id,row.year,row.month,row.day,row.dep_time),
                                                             (row.time,row.lat,row.lon,row.eta,label,row.arpt,row.craft_type)))

    delayedflights = delayedflights.groupByKey().mapValues(list)

    delayedflights.cache()
    delaytimeperflight = delayedflights.map(get_delay_time)
    delaytimeperflight.cache()
    maxdelay = delaytimeperflight.reduce(lambda a,b: get_min_max(a,b))
    mindelay = delaytimeperflight.reduce(lambda a,b: get_min_max(a,b,find_max=False))
    (sumdelay,countdelay) = delaytimeperflight.map(lambda x: (x[6],1)).reduce(tuple_sum)
    avgdelay = sumdelay/countdelay
    delaytimeperflight.unpersist()



    print 'Number of delayed flights: ' + str(delayedflights.count())

    if not os.path.isdir('result'):
        os.mkdir('result')
    fout = open('result/delay.txt', 'w')
    fout.write('Number of delayed flights: ' + str(delayedflights.count()) + '\n')
    fout.write('Average Delay Time: ' + str(avgdelay) + '\n')
    fout.write('------------------------------------------------------\n')
    fout.write('Max Delay:\n')
    fout.write('------------------------------------------------------\n')
    fout.write('Flight ID: ' + str(maxdelay[0]) + '\n')
    fout.write('Date: ' + str(maxdelay[1]) + '-' + str(maxdelay[2]) + '-' + str(maxdelay[3]) + '\n')
    fout.write('Departure Time: ' + str(maxdelay[4]) + '\n')
    fout.write('Craft Type: ' + str(maxdelay[5]) + '\n')
    fout.write('Delay Time: ' + str(maxdelay[6]) + '\n')
    fout.write('------------------------------------------------------\n')
    fout.write('Min Delay:\n')
    fout.write('------------------------------------------------------\n')
    fout.write('Flight ID: ' + str(mindelay[0]) + '\n')
    fout.write('Date: ' + str(mindelay[1]) + '-' + str(mindelay[2]) + '-' + str(mindelay[3]) + '\n')
    fout.write('Departure Time: ' + str(mindelay[4]) + '\n')
    fout.write('Craft Type: ' + str(mindelay[5]) + '\n')
    fout.write('Delay Time: ' + str(mindelay[6]) + '\n')
    fout.write('######################################################\n\n')

    

    for val in delayedflights.collect():
        key = val[0]
        value_list = val[1]
        value_list.sort(key=lambda tup: tup[0])

        comment = 'Flight ID: ' + key[0] + '\\n'
        comment+= ('Date: ' + str(key[1]) + '-' + str(key[2]) + '-' + str(key[3]) + '\\n')
        comment+= ('Departed Time: ' + str(key[4]) + '\\n')
        comment+= ('ETA: ' + str(value_list[0][3]) + '\\n')
        comment+= ('Airport: ' + str(value_list[0][5]) + '\\n')

        fout.write('Flight ID: ' + key[0] + '\n')
        fout.write('Date: ' + str(key[1]) + '-' + str(key[2]) + '-' + str(key[3]) + '\n')
        fout.write('Departed Time: ' + str(key[4]) + '\n')

        lats = []
        lons = []
        found_r = False
        for tup in value_list:
            time = tup[0]
            lat = tup[1]
            lon = tup[2]
            label = tup[4]
            if len(lats) < 1:
                lats.append(lat)
                lons.append(lon)
            elif len(lats) < 2:
                lats.append(lat)
                lons.append(lon)
                gmap.plot(lats,lons,color=label, edge_width=5)
            else:
                lats = lats[1:2]
                lats.append(lat)
                lons = lons[1:2]
                lons.append(lon)
                gmap.plot(lats,lons,color=label, edge_width=5)

            if label == 'r' and found_r == False:
                comment+= ('Time: ' + str(time) + '\\n')
                gmap.scatter([lat],[lon],comments=[comment],color='#0008B' ,marker=True)
                found_r = True

            fout.write('Time: ' + ("%.2f" % time) + '\tLat: ' + ("%.2f" % lat) + '\tLon: ' + ("%.2f" % lon) + '\tStatus: ')
            if label == 'r':
                fout.write('Delayed')
            else:
                fout.write('On-time')
            fout.write('\n')
        fout.write('\n\n')

    gmap.draw('/fas-info/cs/people/GradStudents/jsarria/personal/delay.html')
    gmap.draw('result/delay.html')

    return True
########################################################################################################################
def main(args):

    path = get_path(args[0])
    date = args[0].strip().split('/')

    airports, commands = handle_args(args)
    print 'Airports: ' + str(airports)
    flightdata = sc.textFile(path).map(parse_flights)
    airportdata = get_airport_data(airports)
    i = 0.5
    if len(airports) == 1:
        i = 1.5
    (lat_range, lon_range) = get_area(airportdata,i)


    flightdata = flightdata.filter(lambda x: x['lat'] >= (lat_range['min']-1) and x['lat'] <= (lat_range['max']+1)
                        and x['lon'] >= (lon_range['min']-1) and x['lon'] <= (lon_range['max']+1))

    boundary_paths = get_boundary_paths({'lat': lat_range, 'lon': lon_range})
    ctr_lat = (lat_range['min']+lat_range['max'])/2
    ctr_lon = (lon_range['min']+lon_range['max'])/2

    gmap = GoogleMapPlotter(ctr_lat, ctr_lon, 8)
    for path in boundary_paths:
        gmap.plot(path['lat_range'], path['lon_range'], 'cornflowerblue', edge_width=10)


    if len(airports) >= 2:
        alldata = flightdata.filter(lambda x: x['arr_airport'] in airports and x['dep_airport'] in airports)
    else:
        alldata = flightdata.filter(lambda x: x['arr_airport'] in airports)

    alldata.cache()
    detect_info = detect_delay(alldata,airports,gmap)
########################################################################################################################


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1:len(sys.argv)])
    else:
        print 'Some instructions here'

