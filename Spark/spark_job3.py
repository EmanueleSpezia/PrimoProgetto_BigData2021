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
    .appName("Spark Job 3") \
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
values_filtered_RDD = values_RDD.filter(f=lambda line: (int(line[7][:4]) == 2017))

# Structure: ticker, (close_price, date)
final_values_RDD = values_filtered_RDD.map(f=lambda line: (line[0], (line[2], line[7])))

# First close_price and associated date
first_price_ticker_RDD = final_values_RDD.map(f=lambda l: ((l[0], (dt.datetime.strptime(l[1][1], "%Y-%m-%d")).month),
                                                           (l[1][0], l[1][1]))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (isAfter(x[1], y[1]) is False) else (y[0], y[1]))

# Last close_price and associated date
last_price_ticker_RDD = final_values_RDD.map(f=lambda l: ((l[0], (dt.datetime.strptime(l[1][1], "%Y-%m-%d")).month),
                                                          (l[1][0], l[1][1]))) \
    .reduceByKey(func=lambda x, y: (x[0], x[1]) if (isAfter(x[1], y[1]) is True) else (y[0], y[1]))

# Structure: (ticker, month), %var
percentage_variation_ticker_RDD = last_price_ticker_RDD.join(first_price_ticker_RDD) \
    .map(f=lambda x: ((x[0][0], x[0][1]), percentageVar(x[1])))

# January -> ticker, %var
ticker_var_jan_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 1) \
    .map(f=lambda l: (l[0][0], l[1]))
# February -> ticker, %var
ticker_var_feb_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 2) \
    .map(f=lambda l: (l[0][0], l[1]))
# March -> ticker, %var
ticker_var_mar_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 3) \
    .map(f=lambda l: (l[0][0], l[1]))
# April -> ticker, %var
ticker_var_apr_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 4) \
    .map(f=lambda l: (l[0][0], l[1]))
# May -> ticker, %var
ticker_var_may_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 5) \
    .map(f=lambda l: (l[0][0], l[1]))
# June -> ticker, %var
ticker_var_jun_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 6) \
    .map(f=lambda l: (l[0][0], l[1]))
# July -> ticker, %var
ticker_var_jul_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 7) \
    .map(f=lambda l: (l[0][0], l[1]))
# August -> ticker, %var
ticker_var_aug_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 8) \
    .map(f=lambda l: (l[0][0], l[1]))
# September -> ticker, %var
ticker_var_sep_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 9) \
    .map(f=lambda l: (l[0][0], l[1]))
# October -> ticker, %var
ticker_var_oct_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 10) \
    .map(f=lambda l: (l[0][0], l[1]))
# November -> ticker, %var
ticker_var_nov_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 11) \
    .map(f=lambda l: (l[0][0], l[1]))
# December -> ticker, %var
ticker_var_dec_RDD = percentage_variation_ticker_RDD.filter(f=lambda line: int(line[0][1]) == 12) \
    .map(f=lambda l: (l[0][0], l[1]))

# Create a map for every RDD
ticker_var_jan = ticker_var_jan_RDD.collectAsMap()
ticker_var_feb = ticker_var_feb_RDD.collectAsMap()
ticker_var_mar = ticker_var_mar_RDD.collectAsMap()
ticker_var_apr = ticker_var_apr_RDD.collectAsMap()
ticker_var_may = ticker_var_may_RDD.collectAsMap()
ticker_var_jun = ticker_var_jun_RDD.collectAsMap()
ticker_var_jul = ticker_var_jul_RDD.collectAsMap()
ticker_var_aug = ticker_var_aug_RDD.collectAsMap()
ticker_var_sep = ticker_var_sep_RDD.collectAsMap()
ticker_var_oct = ticker_var_oct_RDD.collectAsMap()
ticker_var_nov = ticker_var_nov_RDD.collectAsMap()
ticker_var_dec = ticker_var_dec_RDD.collectAsMap()

# Create and fill a dict of variation and month
ticker_variation_dict = {}
for el in ticker_var_jan:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][1] = ticker_var_jan[el]
    else:
        ticker_variation_dict[el][1] = ticker_var_jan[el]

for el in ticker_var_feb:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][2] = ticker_var_feb[el]
    else:
        ticker_variation_dict[el][2] = ticker_var_feb[el]

for el in ticker_var_mar:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][3] = ticker_var_mar[el]
    else:
        ticker_variation_dict[el][3] = ticker_var_mar[el]

for el in ticker_var_apr:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][4] = ticker_var_apr[el]
    else:
        ticker_variation_dict[el][4] = ticker_var_apr[el]

for el in ticker_var_may:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][5] = ticker_var_may[el]
    else:
        ticker_variation_dict[el][5] = ticker_var_may[el]

for el in ticker_var_jun:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][6] = ticker_var_jun[el]
    else:
        ticker_variation_dict[el][6] = ticker_var_jun[el]

for el in ticker_var_jul:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][7] = ticker_var_jul[el]
    else:
        ticker_variation_dict[el][7] = ticker_var_jul[el]

for el in ticker_var_aug:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][8] = ticker_var_aug[el]
    else:
        ticker_variation_dict[el][8] = ticker_var_aug[el]

for el in ticker_var_sep:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][9] = ticker_var_sep[el]
    else:
        ticker_variation_dict[el][9] = ticker_var_sep[el]

for el in ticker_var_oct:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][10] = ticker_var_oct[el]
    else:
        ticker_variation_dict[el][10] = ticker_var_oct[el]

for el in ticker_var_nov:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][11] = ticker_var_nov[el]
    else:
        ticker_variation_dict[el][11] = ticker_var_nov[el]

for el in ticker_var_dec:
    if el not in ticker_variation_dict:
        ticker_variation_dict[el] = {}
        ticker_variation_dict[el][12] = ticker_var_dec[el]
    else:
        ticker_variation_dict[el][12] = ticker_var_dec[el]

# Calculate the ticker witch percentage variation is similar for all months (threshold = 3%)
ignore_ticker = {}
final_results = {}
couple_number = 0
for ticker in ticker_variation_dict:
    ignore_ticker[ticker] = ticker
    for ticker_compare in ticker_variation_dict:
        count = 0
        if (ticker_compare != ticker) and (ticker_compare not in ignore_ticker.keys()):
            for m in range(1, 13):
                if (m in ticker_variation_dict[ticker].keys()) and (m in ticker_variation_dict[ticker_compare].keys()):
                    if (0 <= (ticker_variation_dict[ticker][m] - ticker_variation_dict[ticker_compare][m]) <= 3) or (
                            -3 <= (ticker_variation_dict[ticker][m] - ticker_variation_dict[ticker_compare][m]) <= 0):
                        count += 1

                    if count == 12:
                        couple_number += 1
                        final_results[couple_number] = ('GEN: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_variation_dict[ticker][1])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][1])) + '%' + ", "
                                                                                                                   'FEB: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                2])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][2])) + '%' + ", "
                                                                                                                   'MAR: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                3])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][3])) + '%' + ", "
                                                                                                                   'APR: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                4])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][4])) + '%' + ", "
                                                                                                                   'MAG: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                5])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][5])) + '%' + ", "
                                                                                                                   'GIU: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                6])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][6])) + '%' + ", "
                                                                                                                   'LUG: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                7])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][7])) + '%' + ", "
                                                                                                                   'AGO: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                8])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][8])) + '%' + ", "
                                                                                                                   'SET: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                9])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][9])) + '%' + ", "
                                                                                                                   'OTT: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                10])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][10])) + '%' + ", "
                                                                                                                    'NOV: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                11])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][11])) + '%' + ", "
                                                                                                                    'DEC: ' + ticker + ' ' + (
                                                            "{:.2f}".format(ticker_variation_dict[ticker][
                                                                                12])) + '%, ' + ticker_compare + ' ' + (
                                                            "{:.2f}".format(
                                                                ticker_variation_dict[ticker_compare][12])) + '%')

# Final result output
results_RDD = spark.sparkContext.parallelize(list(final_results.items()))
results_final_RDD = results_RDD.map(lambda x: (dict([x])))
results_final_RDD.coalesce(1).saveAsTextFile(output_filepath)