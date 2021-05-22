#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession
import datetime as dt
import re


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


# Calculate the total percentage variation
def percentageVarTot(closes):
    last_close = float(closes[0])
    first_close = float(closes[1])
    percentage = round(((last_close - first_close) / first_close) * 100, 2)
    return percentage


# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--input_path_stocks", type=str, help="Input file path 2")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Parse arguments
args = parser.parse_args()
input_filepath, input_filepath_stocks, output_filepath = args.input_path, args.input_path_stocks, args.output_path

# Initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark Job 2") \
    .config("spark.executor.memory", "2500m") \
    .getOrCreate()

# Read the input file and obtain an RDD with a record for each line
input_data = spark.sparkContext.textFile(input_filepath).cache()
input_data_stocks = spark.sparkContext.textFile(input_filepath_stocks).cache()

# [0 ticker, 1 open_price, 2 close_price, 3 adj_close, 4 low, 5 high, 6 volume,  7 date]

# [0 ticker, 1 exchange, 2 name, 3 sector, 4 industry]

# Convert the input_data lines to list
list_RDD = input_data.map(f=lambda line: line.split(","))
list_stocks_RDD = input_data_stocks.map(f=lambda l: re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", l))

# Skip first line (header)
header_RDD = list_RDD.first()
values_RDD = list_RDD.filter(f=lambda l: l != header_RDD)
values_filtered_RDD = values_RDD.filter(f=lambda l: (int(l[7][:4]) >= 2009) and (int(l[7][:4]) <= 2018))

# Skip first line (header)
header_stocks_RDD = list_stocks_RDD.first()
values_stocks_RDD = list_stocks_RDD.filter(f=lambda l: l != header_stocks_RDD) \
    .filter(f=lambda l: l[3] != 'N/A')

# [0 ticker, 1 close_price, 2 volume, 3 date]
final_values_RDD = values_filtered_RDD.map(f=lambda l: (l[0], (l[2], l[6], l[7])))
# [0 ticker, 1 sector]
final_values_stocks_RDD = values_stocks_RDD.map(f=lambda l: (l[0], l[3]))

# [(sector,year) , (ticker, close, volume, data)]
joined_RDD = final_values_RDD.join(final_values_stocks_RDD).map(f=lambda l: ((l[1][1], int(l[1][0][2][:4])),
                                                                             (str(l[0]), float(l[1][0][0]),
                                                                              int(l[1][0][1]), l[1][0][2])))

# Calculate the first and last close for every ticker in every year
first_price_ticker_RDD = joined_RDD.map(f=lambda l: ((l[0], l[1][0]), (l[1][1], l[1][3]))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (isAfter(x[1], y[1]) is False) else (y[0], y[1]))

last_price_ticker_RDD = joined_RDD.map(f=lambda l: ((l[0], l[1][0]), (l[1][1], l[1][3]))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (isAfter(x[1], y[1]) is True) else (y[0], y[1]))

# Percentage change in the price of the sector in the year
total_first_close = first_price_ticker_RDD.map(f=lambda l: ((l[0][0][0], l[0][0][1]), float(l[1][0]))) \
    .reduceByKey(func=lambda x, y: x + y)

total_last_close = last_price_ticker_RDD.map(f=lambda l: ((l[0][0][0], l[0][0][1]), float(l[1][0]))) \
    .reduceByKey(func=lambda x, y: x + y)

percentage_variation_RDD = total_last_close.join(total_first_close) \
    .map(f=lambda l: ((l[0][0], l[0][1]), percentageVarTot(l[1])))

# Ticker of the sector that had the greater percentage increase in the year
percentage_variation_ticker_RDD = last_price_ticker_RDD.join(first_price_ticker_RDD) \
    .map(f=lambda l: ((l[0][0][0], l[0][0][1]), (l[0][1], percentageVar(l[1])))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (x[1] > y[1]) else (y[0], y[1]))

# Ticker of the sector that had the highest volume of transactions in the year
total_volume_ticker_RDD = joined_RDD.map(f=lambda l: ((l[0], l[1][0]), int(l[1][2]))) \
    .reduceByKey(func=lambda x, y: x + y) \
    .map(f=lambda l: ((l[0][0][0], l[0][0][1]), (l[0][1], int(l[1])))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (x[1] > y[1]) else (y[0], y[1]))

results = percentage_variation_RDD.join(percentage_variation_ticker_RDD).join(total_volume_ticker_RDD)

results_formatted = results.map(f=lambda l: (l[0][0], l[0][1], l[1][0][0], l[1][0][1][0],
                                             l[1][0][1][1], l[1][1][0], l[1][1][1])) \
    .sortBy(lambda l: l[0], ascending=True)

results_formatted.coalesce(1).saveAsTextFile(output_filepath)
