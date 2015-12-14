
from pyspark import SparkContext, SparkConf, SQLContext, rdd
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
from gmplot import GoogleMapPlotter


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

def concatenate_list(a,b):
    return a+b


def detect_delay(alldata, airports, sqlContext,gmap):

    delayedflights = alldata.filter(lambda x: is_late(x['time'],x['eta'],x['dep_time']))
    delayedflights = delayedflights.map(lambda x: ((x['flight_id'],x['year'],x['month'],x['day'],x['dep_time']),1))
    delayedflights = delayedflights.reduceByKey(reduce_key)
    delayedflights = delayedflights.map(lambda ((fid,year,month,day,dep_time),num): (fid,year,month,day,dep_time))

    allflights = alldata.map(lambda x: (x['flight_id'],x['year'],x['month'],x['day'],x['time'],x['lat'],x['lon'],
                                        x['dep_time'],x['eta'],x['arr_airport']))

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
    ])

    sqlContext.createDataFrame(delayedflights,schema=delay_schema).registerTempTable('dt')
    sqlContext.createDataFrame(allflights,schema=flight_schema).registerTempTable('ft')

    delay_df = sqlContext.sql("""
        SELECT ft.flight_id AS flight_id, ft.year AS year, ft.month AS month, ft.day AS day, ft.time AS time, ft.lat AS lat, ft.lon AS lon, ft.dep_time AS dep_time, ft.eta AS eta, ft.arpt AS arpt
        FROM ft
        INNER JOIN dt
        ON ft.flight_id = dt.flight_id AND ft.year = dt.year AND ft.month = dt.month AND ft.day = dt.day AND ft.dep_time = dt.dep_time
    """)

    delayedflights = delay_df.rdd.map(lambda row: (row,label(row.time,row.eta,row.dep_time)))
    delayedflights = delayedflights.map(lambda (row,label): ((row.flight_id,row.year,row.month,row.day,row.dep_time),
                                                             [(row.time,row.lat,row.lon,row.eta,label, row.arpt)] ))

    delayedflights = delayedflights.reduceByKey(concatenate_list)
    print 'Number of delayed flights: ' + str(delayedflights.count())
    alldata.unpersist()



    for val in delayedflights.collect():
        key = val[0]
        value_list = val[1]
        value_list.sort(key=lambda tup: tup[0])

        comment = 'Flight ID: ' + key[0] + '\\n'
        comment+= ('Date: ' + str(key[1]) + '-' + str(key[2]) + '-' + str(key[3]) + '\\n')
        comment+= ('Departed Time: ' + str(key[4]) + '\\n')
        comment+= ('ETA: ' + str(value_list[0][3])) + '\\n'
        comment+= ('Airport: ' + str(value_list[0][5])) + '\\n'

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
                print 'Making a marker!'
                comment+= ('Time: ' + str(time) + '\\n')
                gmap.scatter([lat],[lon],comments=[comment],color='#3B0B39' ,marker=True)
                found_r = True

    gmap.draw('/fas-info/cs/people/GradStudents/jsarria/personal/delay.html')
    gmap.draw('delay.html')
