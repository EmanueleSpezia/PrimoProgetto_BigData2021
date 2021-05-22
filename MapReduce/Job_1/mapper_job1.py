#!/usr/bin/env python3
"""mapper.py"""

import sys
from datetime import datetime as dt

# map each ticker to its first listing on the stock exchange
first_listing = {}

# map each ticker to its last listing on the stock exchange
last_listing = {}

# map each ticker to the first and the last close price
first_close = {}
last_close = {}

# Variation
variation = {}

# map each ticker to its price to calculate the min and max price
max_price = {}
min_price = {}

# map each ticker to its 
date_growing = {}
days_growing = {}
year_growing = {}
# Days for exchange:
# 0 - Monday
# 1 - Tuesday
# 2 - Wednesday
# 3 - Thursday
# 4 - Friday

input_file = sys.stdin

# Not read the first line of input file
next(input_file)

# Read lines from input_file
for line in input_file:

    # Removing leading/trailing whitespaces
    line = line.strip()

    # Parse the input elements
    ticker, open_price_str, close_price_str, adj_close, low_str, high_str, volume, date_str = line.split(",")

    # Convert String to Date
    date_dt = dt.strptime(date_str, '%Y-%m-%d')
    # Convert String to float
    open_price = float(open_price_str)
    close_price = float(close_price_str)
    low = float(low_str)
    high = float(high_str)

    # Find the first listing and close price
    if ticker not in first_listing:
        first_listing[ticker] = date_dt
        first_close[ticker] = close_price
    else:
        if date_dt < first_listing[ticker]:
            first_listing[ticker] = date_dt
            first_close[ticker] = close_price

    # Find the last listing and close price
    if ticker not in last_listing:
        last_listing[ticker] = date_dt
        last_close[ticker] = close_price
    else:
        if date_dt > last_listing[ticker]:
            last_listing[ticker] = date_dt
            last_close[ticker] = close_price

    # Calculate the max price of a ticker
    if ticker not in max_price:
        max_price[ticker] = close_price
    else:
        if close_price > max_price[ticker]:
            max_price[ticker] = close_price

    # Calculate the min price of a ticker
    if ticker not in min_price:
        min_price[ticker] = close_price
    else:
        if close_price < min_price[ticker]:
            min_price[ticker] = close_price

    # Find the dates on which the ticker grew
    if close_price - open_price > 0:
        if ticker not in date_growing:
            date_growing[ticker] = []
            date_growing[ticker].append(date_dt)
        else:
            date_growing[ticker].append(date_dt)

# Convert Date to String
for ticker in first_listing.keys():
    first_listing[ticker] = dt.strftime(first_listing[ticker], '%Y-%m-%d')

for ticker in last_listing.keys():
    last_listing[ticker] = dt.strftime(last_listing[ticker], '%Y-%m-%d')

# Final calculation of consecutive days in which the ticker has grown with the associated year
for ticker in date_growing.keys():
    sorted_date = sorted(date_growing[ticker])
    max_year = 0
    # Initialization
    days_count = 1
    max_days = 1

    for i in range(1, len(sorted_date)):
        if (sorted_date[i].weekday() - sorted_date[i - 1].weekday()) == 1 or (
                sorted_date[i].weekday() - sorted_date[i - 1].weekday()) == -4:
            days_count += 1
        else:
            if days_count > max_days:
                max_days = days_count
                max_year = sorted_date[i].year
                days_count = 1
            else:
                days_count = 1

    #
    days_growing[ticker] = max_days
    year_growing[ticker] = max_year

# Calculation of the percentage change in the 
for ticker in first_close:
    variation[ticker] = ((last_close[ticker] - first_close[ticker]) / first_close[ticker]) * 100

# Final results print
for ticker in first_listing.keys():
    if ticker in days_growing.keys():
        print('%s\t%s\t%s\t%f\t%f\t%f\t%s\t%i' % (
            ticker, first_listing[ticker], last_listing[ticker], variation[ticker], max_price[ticker],
            min_price[ticker],
            days_growing[ticker], year_growing[ticker]))
