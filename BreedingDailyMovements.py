#!/usr/bin/env python
# encoding: utf-8
"""
BreedingDailyMovements.py

Created by Brant Faircloth on 2009-10-06.
Copyright (c) 2009 Brant Faircloth. All rights reserved.

Computes the minimum daily movements of individuals by bird id using telemetry
locations.

August 22 {2001,2002,2003} is the earliest stop date encountered in all years.
Stop dates for BCF telemetry:

2001-08-22
2002-08-29
2003-08-28

"""

import pdb
import datetime
import math
import numpy
import psycopg2 as pspg

def EuclideanDistance(pos1, pos2):
    '''Determine the distance btw. 2 Cartesian points'''
    x1, y1 = pos1
    x2, y2 = pos2
    return math.sqrt((x1 - x2)**2 + (y1 - y2)**2)

def ConfidenceInterval(distance):
    '''Compute the 95% confidence interval for the average distance an
    individual moved'''
    return (distance.std()/math.sqrt(len(distance)))*1.96

def GetDailyDistance(records):
    '''Determine the daily distance travelled.  Days must be 1 unit apart
    (i,e. consecutive).  Distance is Euclidean, and results are stored in an 
    array, which makes summarization very rapid'''
    distance = numpy.array([])
    for day in range(len(records)):
        if day+1 < len(records) and abs(records[day][0] - records[day+1][0]) == datetime.timedelta(1):
            distance = numpy.append(distance, EuclideanDistance(records[day][1:3], records[day+1][1:3]))
    confidence = ConfidenceInterval(distance)
    return distance.mean(), confidence

def GetBirdList(cursor, year):
    '''get distinct list of all birds in Genetics Area.  Telemetry table
    in Postgres only contains birds in Genetics Area and found btw. 
    4/15/XXXX and 10/15/XXXX where XXXX = 2001|2002|2003'''
    cursor.execute('''SELECT DISTINCT(id) FROM telemetry WHERE 
    location_type = 'Location' AND date_part('year', date) = %s ORDER BY id''', (year,))
    return cursor.fetchall()

def GetBirdRecords(cursor, year, bird):
    '''Get records for individuals birds by date with avg x and y coords'''
    cursor.execute("""SELECT date, AVG(x_coord), AVG(y_coord) FROM telemetry WHERE 
    id = '%s' AND location_type = 'Location' AND date between 
    '%s-04-15' AND '%s-08-15' AND x_coord IS NOT NULL AND y_coord 
    IS NOT NULL GROUP BY date ORDER BY date""" % (bird[0], year, year))
    return cursor.fetchall()

def GetBirdSex(cursor, b):
    '''Determine the sex of a bird'''
    cursor.execute("""SELECT sex FROM birds WHERE id = '%s'""" % b)
    return cursor.fetchall()[0]

def CreateDistanceTable(cursor):
    '''Create a table for the data'''
    #pdb.set_trace()
    #try:
    cursor.execute("""CREATE TABLE distance (id smallint REFERENCES birds(id), year char(4), 
    distance float, confidence float)""")
    #except:
    #    cursor.execute("""TRUNCATE TABLE distance""")

def InsertDbRecord(cursor, b, y, dist, confidence):
    '''Insert the result to the table we created'''
    cursor.execute("""INSERT INTO distance (id, year, distance, confidence) 
    VALUES (%s, %s, %s, %s)""" % (b, y, dist, confidence))

def main():
    #years = [2001]
    years = [2001, 2002, 2003]
    # connect to dbase
    conn = pspg.connect("dbname='quail' host='localhost'")
    cursor = conn.cursor()
    CreateDistanceTable(cursor)
    for y in years:
        bird_list = GetBirdList(cursor, y)
        for b in bird_list:
            records = GetBirdRecords(cursor, y, b)
            daily_dist_mean, daily_dist_confidence = GetDailyDistance(records)
            sex = GetBirdSex(cursor, b)
            if not numpy.isnan(daily_dist_mean):
                InsertDbRecord(cursor, b[0], y, daily_dist_mean, daily_dist_confidence)
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()

