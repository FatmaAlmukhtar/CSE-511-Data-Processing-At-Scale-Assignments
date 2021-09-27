#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    c = openconnection.cursor()

    c.execute("SELECT COUNT(*) FROM RangeRatingsMetadata;")
    num_of_partitions = c.fetchone()[0]

    range_result = []
    for item in range(num_of_partitions):
        c.execute("SELECT 'RangeRatingsPart" + str(item) + "' AS PartitionNum, UserID, MovieID, Rating FROM RangeRatingsPart" + str(item) +
                  " WHERE Rating>=" + str(ratingMinValue) + " AND Rating<=" + str(ratingMaxValue) + ";")
        for i in c.fetchall():
            range_result.append(i)

    c.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata;")
    num_of_partitions = c.fetchone()[0]

    rrobin_result = []
    for item in range(num_of_partitions):
        c.execute("SELECT 'RoundRobinRatingsPart" + str(item) + "' AS PartitionNum, UserID, MovieID, Rating FROM RoundRobinRatingsPart" + str(item) +
                  " WHERE Rating>=" + str(ratingMinValue) + " AND Rating<=" + str(ratingMaxValue) + ";")
        for i in c.fetchall():
            rrobin_result.append(i)

    path_prefix = str(os.path.dirname(__file__))
    if os.path.exists(path_prefix + '/RangeQueryOut.txt'):
        os.remove(path_prefix + '/RangeQueryOut.txt')
    writeToFile(path_prefix + '/RangeQueryOut.txt', range_result + rrobin_result)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    c = openconnection.cursor()

    c.execute("SELECT COUNT(*) FROM RangeRatingsMetadata;")
    num_of_partitions = c.fetchone()[0]

    range_result = []
    for item in range(num_of_partitions):
        c.execute("SELECT 'RangeRatingsPart" + str(
            item) + "' AS PartitionNum, UserID, MovieID, Rating FROM RangeRatingsPart" + str(item) +
                  " WHERE Rating=" + str(ratingValue) + ";")
        for i in c.fetchall():
            range_result.append(i)

    c.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata;")
    num_of_partitions = c.fetchone()[0]

    rrobin_result = []
    for item in range(num_of_partitions):
        c.execute("SELECT 'RoundRobinRatingsPart" + str(item) + "' AS PartitionNum, UserID, MovieID, Rating FROM RoundRobinRatingsPart" + str(item) +
                  " WHERE Rating=" + str(ratingValue) + ";")
        for i in c.fetchall():
            rrobin_result.append(i)

    path_prefix = str(os.path.dirname(__file__))
    if os.path.exists(path_prefix + '/PointQueryOut.txt'):
        os.remove(path_prefix + '/PointQueryOut.txt')
    writeToFile(path_prefix + '/PointQueryOut.txt', range_result + rrobin_result)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
