#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession
import datetime as dt


# Support functions
def isAfter(date_str_1, date_str_2):
    date1 = dt.datetime.strptime(date_str_1, "%Y-%m-%d")
    date2 = dt.datetime.strptime(date_str_2, "%Y-%m-%d")
    return date1 > date2


# Calculate the percentage variation
def percentageVar(closes):
    last_close = float(closes[0][0])
    first_close = float(closes[1][0])
    percentage = round(((last_close - first_close) / first_close) * 100, 2)
    return percentage


# Given a list of dates calculates the consecutive days on which the stock grows
def maxDaysGrowing(date_list):
    sorted_list = sorted(date_list)
    # Initialization
    days_count = 1
    max_days = 1

    for i in range(1, len(sorted_list)):
        if (sorted_list[i].weekday() - sorted_list[i - 1].weekday()) == 1 or (
                sorted_list[i].weekday() - sorted_list[i - 1].weekday()) == -4:
            days_count += 1
        else:
            if days_count > max_days:
                max_days = days_count
                days_count = 1
            else:
                days_count = 1

    return str(max_days)


# Given a list of dates calculates the year in which there are several consecutive days in which the stock grows
def yearMaxGrowing(date_list):
    sorted_list = sorted(date_list)
    max_year = sorted_list[0].year
    # Initialization
    days_count = 1
    max_days = 1

    for i in range(1, len(sorted_list)):
        if (sorted_list[i].weekday() - sorted_list[i - 1].weekday()) == 1 or (
                sorted_list[i].weekday() - sorted_list[i - 1].weekday()) == -4:
            days_count += 1
        else:
            if days_count > max_days:
                max_days = days_count
                max_year = sorted_list[i].year
                days_count = 1
            else:
                days_count = 1
    return max_year


# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark Job 1") \
    .config("spark.executor.memory", "2500m") \
    .getOrCreate()

# Read the input file and obtain an RDD with a record for each line
input_data = spark.sparkContext.textFile(input_filepath).cache()

# [0 ticker, 1 open_price, 2 close_price, 3 adj_close, 4 low, 5 high, 6 volume,  7 date]

# Convert the input_data lines to list
list_RDD = input_data.map(f=lambda line: line.split(","))

# Skip first line (header)
header_RDD = list_RDD.first()
values_RDD = list_RDD.filter(f=lambda line: line != header_RDD)

# First close_price and associated date
first_quot_RDD = values_RDD.map(f=lambda line: (line[0], (line[2], line[7]))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (isAfter(x[1], y[1]) is False) else (y[0], y[1]))

# Last close_price and associated date
last_quot_RDD = values_RDD.map(f=lambda line: (line[0], (line[2], line[7]))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (isAfter(x[1], y[1]) is True) else (y[0], y[1]))

# General percentage variation
percentage_variation_RDD = last_quot_RDD.join(first_quot_RDD) \
    .map(f=lambda x: (x[0], percentageVar(x[1])))

# Min close_price
min_price_RDD = values_RDD.map(f=lambda line: (line[0], round(float(line[1]), 2))) \
    .reduceByKey(func=lambda x, y: min(x, y))

# Max close_price
max_price_RDD = values_RDD.map(f=lambda line: (line[0], round(float(line[1]), 2))) \
    .reduceByKey(func=lambda x, y: max(x, y))

# Dates when ticker grew
ticker_grew_RDD = values_RDD.map(f=lambda line: (line[0], float(line[1]), float(line[2]), line[7])) \
    .filter(f=lambda line: ((line[2] - line[1]) > 0)) \
    .map(f=lambda line: (line[0], dt.datetime.strptime(line[3], "%Y-%m-%d"))) \
    .groupByKey() \
    .map(f=lambda line: (line[0], (maxDaysGrowing(list(line[1])), yearMaxGrowing(list(line[1])))))

results = first_quot_RDD.join(last_quot_RDD).join(percentage_variation_RDD).join(min_price_RDD).join(max_price_RDD) \
    .join(ticker_grew_RDD)

results_formatted = results.map(f=lambda x: (x[0], x[1][0][0][0][0][0][1], x[1][0][0][0][0][1][1], x[1][0][0][0][1],
                                             x[1][0][1], x[1][0][0][1], x[1][1][0], x[1][1][1])) \
    .sortBy(lambda x: x[1], ascending=False).sortBy(lambda x: x[0], ascending=False)

results_formatted.coalesce(1).saveAsTextFile(output_filepath)
