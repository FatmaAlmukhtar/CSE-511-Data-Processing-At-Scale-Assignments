#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    c = openconnection.cursor()
    c.execute("DROP TABLE IF EXISTS " + ratingstablename)
    c.execute("CREATE TABLE " + ratingstablename + "(UserID INT, MovieID INT, Rating FLOAT);")
    with open(ratingsfilepath) as file:
        for tuple in file:
            userid, movieid, rating, timestamp = tuple.split("::")
            c.execute("INSERT INTO " + ratingstablename + " VALUES(" + str(userid) + ", " + str(movieid) + ", " +  str(rating) + ");")
    openconnection.commit()
    c.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    interval = 5.0/numberofpartitions
    c = openconnection.cursor()
    for i in range(numberofpartitions):
        c.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")
        if i==0:
            c.execute(
                "CREATE TABLE range_part" + str(i) + " AS(SELECT * FROM " + ratingstablename + " WHERE Rating>=0 AND Rating<=" +
                str(interval) + ");")
        else:
            c.execute("CREATE TABLE range_part" + str(i) + " AS(SELECT * FROM " + ratingstablename + " WHERE Rating>" +
                      str(i*interval) + "AND Rating<=" + str((i+1) * interval) + ");")
    openconnection.commit()
    c.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    c = openconnection.cursor()
    for i in range(numberofpartitions):
        c.execute("DROP TABLE IF EXISTS rrobin_part" + str(i))
        c.execute("CREATE TABLE rrobin_part" + str(i) + "(UserID INT, MovieID INT, Rating FLOAT);")
    c.execute("SELECT * FROM " + ratingstablename)
    tuples = c.fetchall()
    part_number = 0
    for t in tuples:
        c.execute("INSERT INTO rrobin_part" + str(part_number) + " VALUES(" + str(t[0]) + ", " + str(t[1]) + ", " + str(t[2]) + ");")
        part_number += 1
        if part_number>=numberofpartitions:
            part_number = 0
    openconnection.commit()
    c.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    c.execute("SELECT * FROM " + ratingstablename)
    num_of_tuples = len(c.fetchall())
    c.execute("INSERT INTO " + ratingstablename + " VALUES(" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")
    c.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%rrobin_part%';")
    num_of_parts = len(c.fetchall())
    c.execute("INSERT INTO rrobin_part" + str(num_of_tuples % num_of_parts) + " VALUES(" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")
    openconnection.commit()
    c.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    c.execute("INSERT INTO " + ratingstablename + " VALUES(" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")
    c.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%range_part%';")
    num_of_parts = len(c.fetchall())
    interval = 5.0/num_of_parts
    for i in range(num_of_parts):
        if i==0:
            if rating<=(i+interval):
                c.execute("INSERT INTO range_part" + str(i) + " VALUES(" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");")
        else:
            if rating>(i*interval) and rating<=((i+1)*interval):
                c.execute(
                    "INSERT INTO range_part" + str(i) + " VALUES(" + str(userid) + ", " + str(itemid) + ", " + str(
                        rating) + ");")
    openconnection.commit()
    c.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
