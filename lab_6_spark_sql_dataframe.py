#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py
'''
import os
import time
# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, percentile_approx, max, countDistinct, avg

def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 6 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'boats.txt')
    boats.createOrReplaceTempView('boats')

    sailors = spark.read.json(f'sailors.json')
    sailors.createOrReplaceTempView('sailors')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    
    reserves = spark.read.json(f'reserves.json')
    reserves.createOrReplaceTempView('reserves')

    ## Example 1
    # Sailors with high ratings - Spark SQL
    start_time = time.time()
    query1 = spark.sql('SELECT sailors.sid, sailors.sname, sailors.rating FROM sailors WHERE sailors.rating > 7')
    query1.show()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Q1: Filter: Spark SQL time: {elapsed_time} seconds")
    
    
    # Sailors with high ratings - Spark
    start_time = time.time()
    results = sailors.filter(col("rating") > 7)\
                       .select("sid", "sname", "rating")
    results.show()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Q1: Filter: Spark Dataframe time: {elapsed_time} seconds")
    
    ## Example 2
    # Get High rating Sailors along with number of boat reservations - Spark SQL
    start_time = time.time()
    query2 = spark.sql('SELECT s.sid, COUNT(r.bid) as num_reserves FROM sailors s JOIN reserves r ON s.sid = r.sid WHERE s.rating > 7 GROUP BY s.sid')
    query2.show()
    end_time = time.time()
    elapsed_time = end_time - start_time 
    print(f"Q2: Join: Spark SQL time: {elapsed_time} seconds")

    # Get High rating Sailors along with number of boat reservations - Spark Object Interface
    start_time = time.time()
    sailors_boats = sailors.join(reserves, "sid")

    sailors_boats = sailors_boats.filter(col("rating") > 7)\
                 .groupBy("sid")\
                 .agg(count("bid").alias("num_reserves")) \
                 .select("sid", "num_reserves")
    sailors_boats.show()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Q2: Spark Dataframe time:  {elapsed_time} seconds")


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user userID from the command line
    # We need this to access the user's folder in HDFS
    userID = os.environ['USER']

    # Call our main routine
    main(spark, userID)
