#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import sqlite3
from pandas import Series, DataFrame, read_sql
import pymysql
from pymysql import cursors
import re

def createSchema(dbName, passWord):
    """ Connect to MYSQL, then create a database """
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 port=3306,
                                 password=passWord)
    try:
        with connection.cursor() as cursor:
            cursor.execute('CREATE SCHEMA {}'.format(dbName))
    finally:
        connection.close()

def createTable(dbName, passWord, createStatement):
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 port=3306,
                                 password=passWord,
                                 db=dbName)
    try:
        with connection.cursor() as cursor:
            sqlQuery = createStatement
            cursor.execute(sqlQuery)
        connection.commit()
    finally:
        connection.close()

def dropTable(dbName, passWord, tableName):
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 port=3306,
                                 password=passWord,
                                 db=dbName)
    try:
        with connection.cursor() as cursor:
            sqlQuery = "DROP TABLE IF EXISTS {}".format(tableName)
            cursor.execute(sqlQuery)
        connection.commit()
    finally:
        connection.close()

def batchInsert(fileName, insertStatement, dbName, tableName, passWord, batchSize=10000, logStep=20):
    """ Read the data from the csv file one line at a time and insert into """
    """ the table one batch at a time """
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 port=3306,
                                 password=passWord,
                                 db=dbName)
    start = time.time()
    with open(fileName) as infile:
        next(infile)  # Skip the header row
        data = []
        count = 0
        batch_count = 0
        for line in infile:
            count += 1
            line = line.rstrip() # Remove newline character if present
            line = re.split(r',\s*(?![^()]*\))', line) # Avoid splitting if between parentheses
            data.append(tuple(list(line)))
            if len(data) == batchSize:
                batch_count += 1
                try:
                    with connection.cursor() as cursor:
                        cursor.executemany(insertStatement, data)
                    connection.commit()
                    if batch_count % logStep == 0:
                        print(batch_count,'Batches successfully inserted into database')
                except BaseException as e:
                    print(e)
                data = []
        # Here, we have to do another INSERT to cover the final rows
        # of the file that did not get put into the last complete batch
        try:
            with connection.cursor() as cursor:
                cursor.executemany(insertStatement, data)
            connection.commit()
        except BaseException as e:
            print(e)
    print('Total number of batches was:', batch_count, 'Job took', time.time() - start, 'seconds')
    print('Total number of rows was:', count)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM {};'.format(tableName))
            result = cursor.fetchall()
            print('Total number of rows successfully inserted:', result)
        connection.commit()
    except BaseException as e:
        print(e)
    connection.close()

def tableHead(tableName, dbName, passWord, N):
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 port=3306,
                                 password=passWord,
                                 db=dbName)
    try:
        with connection.cursor() as cursor:
            sqlQuery = '''SELECT *
            FROM {}
            LIMIT {};'''.format(tableName, N)
            cursor.execute(sqlQuery)
            result = read_sql(sqlQuery, connection)
            print(result)
        connection.commit()
    finally:
        connection.close()

def generateInsertStatement(dbName, tableName, passWord):
    """ Find column names automatically from empty table in database that you have created """
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 port=3306,
                                 password=passWord,
                                 db=dbName)
    try:
        with connection.cursor() as cursor:
            statement = "SHOW columns FROM {};".format(tableName)
            cursor.execute(statement)
            columnNames = [column[0] for column in cursor.fetchall()]
            n = len(columnNames)
            columnNames = ', '.join([x for x in columnNames])
            placeHolders = ''.join(['%s, ' * (n - 1)]) + '%s'
            insertStatement = '''INSERT INTO {} ({}) VALUES ({})'''.format(tableName
                                                                           , columnNames
                                                                           , placeHolders)
        return columnNames, insertStatement
        connection.commit()
    finally:
        connection.close()