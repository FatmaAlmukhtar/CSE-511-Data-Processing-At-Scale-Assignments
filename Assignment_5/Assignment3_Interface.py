#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    c = openconnection.cursor()
    c.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + ";")
    min_val = c.fetchone()[0]
    c.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + ";")
    max_val = c.fetchone()[0]

    numOfThreads = 5
    interval = (max_val - min_val) / float(numOfThreads)

    threads = []
    for i in range(numOfThreads):
        min_limit = min_val + i * interval
        max_limit = min_val + (i + 1) * interval
        thread = threading.Thread(target=Sort_Function, args=(openconnection, i, InputTable, SortingColumnName, min_limit, max_limit))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    c.execute("DROP TABLE IF EXISTS " + OutputTable + ";")
    c.execute("CREATE TABLE " + OutputTable + " (LIKE " + InputTable + ");")
    for i in range(numOfThreads):
        c.execute("INSERT INTO " + OutputTable + " SELECT * FROM temp_table" + str(i) + ";")
        c.execute("DROP TABLE IF EXISTS temp_table" + str(i) + ";")
    openconnection.commit()


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    c = openconnection.cursor()

    c.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";")
    min_1 = c.fetchone()[0]
    c.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";")
    min_2 = c.fetchone()[0]
    min_val = min(min_1, min_2)

    c.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + ";")
    max_1 = c.fetchone()[0]
    c.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + ";")
    max_2 = c.fetchone()[0]
    max_val = max(max_1, max_2)

    numOfThreads = 5
    interval = (max_val - min_val) / float(numOfThreads)

    threads = []
    for i in range(numOfThreads):
        min_limit = min_val + i * interval
        max_limit = min_val + (i + 1) * interval

        thread = threading.Thread(target=Join_Function,
                                  args=(openconnection, i, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, min_limit, max_limit))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    c.execute("DROP TABLE IF EXISTS " + OutputTable + ";")
    c.execute("CREATE TABLE " + OutputTable + " AS(SELECT * FROM " + InputTable1 + ", " + InputTable2 + ") WITH NO DATA;")
    for i in range(numOfThreads):
        c.execute("INSERT INTO " + OutputTable + " SELECT * FROM temp_table" + str(i) + ";")
        c.execute("DROP TABLE IF EXISTS temp_table" + str(i) + ";")
    openconnection.commit()


def Sort_Function (openconnection, i, InputTable, SortingColumnName, min_limit, max_limit):

    c = openconnection.cursor()
    c.execute("DROP TABLE IF EXISTS temp_table" + str(i) + ";")

    if i == 0:
        c.execute(
            "CREATE TABLE temp_table" + str(
                i) + " AS(SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + ">=" + str(min_limit) + "AND " + SortingColumnName + "<=" +
            str(max_limit) + ");")
    else:
        c.execute("CREATE TABLE temp_table" + str(i) + " AS(SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + ">" +
                  str(min_limit) + "AND " + SortingColumnName + "<=" + str(max_limit) + ");")

    openconnection.commit()


def Join_Function(openconnection, i, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, min_limit, max_limit):
    c = openconnection.cursor()

    c.execute("DROP TABLE IF EXISTS temp_table" + str(i) + ";")

    c.execute(
        "CREATE TABLE temp_table" + str(i) +
            " AS(SELECT * FROM " + InputTable1 + " INNER JOIN " + InputTable2 +
            " ON " + InputTable1 + "." + Table1JoinColumn + "=" + InputTable2 + "." + Table2JoinColumn +
            " WHERE " + InputTable1 + "." + Table1JoinColumn + ">=" + str(min_limit) +
            " AND " + InputTable1 + "." + Table1JoinColumn + "<=" + str(max_limit) + ");")

    openconnection.commit()