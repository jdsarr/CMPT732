#from pyspark import SparkContext, SparkConf

'''
conf = SparkConf().setAppName('Test')
sc = SparkContext(conf=conf)
output = sys.argv[1]
data = sc.textFile(output+ '/2004/*/*')
data = data.map(lambda x: x.split('|')).map(lambda x: x[0].split('-'))


print 'Number of counts: ' + str(data.count())
'''
from gmplot.gmplot import GoogleMapPlotter
from pyspark import SparkContext, SparkConf
import sys



def get_path(line):

    path = 'flightdata'

    date = line.strip().split('/')
    if len(date) !=3:
        return None

    year  = date[0]
    month = date[1]
    day   = date[2]

    if year == '':
        year = '*'
        path+='/*'
    elif int(year) < 2000 or int(year) > 2005:
        return None
    else:
        path+= ('/' + year)

    print year
    if month == '':
        month = '*'
        path+='/*'
    elif int(month) < 1 or int(month) > 12:
        return None
    else:
        path+= ('/'+year+month+'m')

    if day == '':
        path+='/*'
    elif int(day) < 1 or int(day) > 31:
        return None
    else:
        if year == month == '*':
            year = ''
        path+= ('/tsmft'+year+month+day+'.bz2')

    return path

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

def get_airport_data(airports):

    conf = SparkConf().setAppName('Test')
    sc   = SparkContext(conf=conf)
    airportdata = open('airports.dat','r').read().split('\n')


    airportdata = sc.parallelize(airportdata).map(parse_airports).filter(lambda x: x['FAA'] in airports
                                                                                   and x['country'] == 'United States')
    airportdata = airportdata.collect()
    if len(airportdata) == 0:
        raise Exception('Error: No airports founds')
    elif len(airports) != len(airportdata):
        raise Exception('Error: At least one airport not found')

    return airportdata

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


#########################################################################################################3

airports = ['SEA']
airportdata = get_airport_data(airports)
i = 0.5
if len(airports) == 1:
    i = 1.5
(lat_range, lon_range) = get_area(airportdata,i)
boundary_paths = get_boundary_paths({'lat': lat_range, 'lon': lon_range})
ctr_lat = (lat_range['min']+lat_range['max'])/2
ctr_lon = (lon_range['min']+lon_range['max'])/2



print sys.argv[1]
path = get_path(sys.argv[1])
if path != None:
    print path
else:
    print "WRONG!"




gmap = GoogleMapPlotter(ctr_lat, ctr_lon, 8, marker_path='')
for path in boundary_paths:
    gmap.plot(path['lat_range'], path['lon_range'], 'cornflowerblue', edge_width=10)

'''
gmap.plot([lat-0.5, lat+0.5], [lon-0.5, lon-0.5], 'cornflowerblue', edge_width=10)
gmap.plot([lat-0.5, lat+0.5], [lon+0.5, lon+0.5], 'cornflowerblue', edge_width=10)
gmap.plot([lat-0.5, lat-0.5], [lon-0.5, lon+0.5], 'cornflowerblue', edge_width=10)
gmap.plot([lat+0.5, lat+0.5], [lon-0.5, lon+0.5], 'cornflowerblue', edge_width=10)
'''
gmap.scatter([37.428, 40], [-122.145, -118], color='#3B0B39', size=40, marker=False)
gmap.scatter([40], [-89.145],comments=['Here\\nThere'], color='r', marker=True)


gmap.draw("mymap.html")
