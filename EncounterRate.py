#!/usr/bin/env python
# encoding: utf-8
"""
EncounterRate.py

Created by Brant Faircloth on 2009-10-08.
Copyright (c) 2009 Brant Faircloth. All rights reserved.

Computed the encounter rate given some minimum distance (radius of a circle)
within which other individuals fall.  Stores counts of "encountered" individuals 
for each id by sex and date.

"""
import pdb
import psycopg2 as pspg

def GetBirdList(cursor, year):
    '''get distinct list of all birds in Genetics Area.  Telemetry table
    in Postgres only contains birds in Genetics Area and found btw. 
    4/15/XXXX and 10/15/XXXX where XXXX = 2001|2002|2003'''
    cursor.execute('''SELECT DISTINCT(id) FROM telemetry WHERE 
    location_type = 'Location' AND date_part('year', date) = %s ORDER BY id'''
    , (year,))
    return cursor.fetchall()

def GetBirdRecords(cursor, year, bird):
    '''Get records for individuals birds by date with avg x and y coords'''
    cursor.execute("""SELECT date, AVG(x_coord), AVG(y_coord) 
    FROM telemetry WHERE id = '%s' AND location_type = 'Location' AND 
    date between '%s-04-15' AND '%s-08-15' AND x_coord IS NOT NULL AND y_coord 
    IS NOT NULL GROUP BY date ORDER BY date""" % (bird[0], year, year))
    return cursor.fetchall()

def GetEncounteredBirds(cursor, bird, record, distance):
    '''Determine the birds that are within `distance` meters of the focus bird
    using spatially aware postgres database call.  Not optimized for speed 
    with a bounding box because it was throwing errors with additional query
    parameters'''
    #pdb.set_trace()
    date, x, y = record
    cursor.execute("""SELECT telemetry.id, birds.sex FROM telemetry, birds 
    WHERE telemetry.id = birds.id AND 
    ST_Distance(geom, ST_GeomFromText('POINT(%s %s)',32617)) < %s 
    AND date = '%s' AND telemetry.id != %s""" % (x, y, distance, date, bird[0]))
    return cursor.fetchall()
    
def ParseEncounters(encounters):
    '''Take the encounter data, and count the number of males and females 
    within'''
    males, females = 0,0
    for e in encounters:
        if e[1] == 'male':
            males += 1
        elif e[1] == 'female':
            females += 1
    return males, females

def CreateEncounterTable(conn, cursor):
    '''Create the table for the data'''
    #try:
    cursor.execute("""CREATE TABLE encounter (id smallint REFERENCES 
    birds(id), date date, males smallint, females smallint)""")
    #except:
    #    print "Truncating `quail.encounter` table"
    #    cursor.execute("""DROP TABLE encounter""")
    #    CreateEncounterTable(conn,cursor)

def main():
    # this is the distance of the radius of our circle. in this case, 
    # it's an average distance a bobwhite moves in a day - not
    # sex-specific beceause the sexes don't appear to differ in distance
    # travelled.
    distance = 131.9
    #years = [2001]
    years = [2001, 2002, 2003]
    # connect to dbase
    conn = pspg.connect("dbname='quail' host='localhost'")
    cursor = conn.cursor()
    CreateEncounterTable(conn, cursor)
    for y in years:
        bird_list = GetBirdList(cursor, y)
        for b in bird_list:
            records = GetBirdRecords(cursor, y, b)
            for r in records:
                date = r[0]
                all_encounters = GetEncounteredBirds(cursor, b, r, distance)
                if all_encounters:
                    males, females = ParseEncounters(all_encounters)
                else:
                    males, females = 0,0
                cursor.execute("""INSERT INTO encounter (id, date, males, 
                females) VALUES (%s, '%s', %s, %s)""" % (b[0], date, males, females))
    cursor.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()

