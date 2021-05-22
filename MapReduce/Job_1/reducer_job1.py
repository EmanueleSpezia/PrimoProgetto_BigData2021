#!/usr/bin/env python3
"""reducer.py"""

import sys

results = {}

# Input comes from STDIN
for line in sys.stdin:

    # Remove leading/trailing spaces
    line = line.strip()

    # Parse the input elements
    ticker, first_quot, last_quot, var, max_price, min_price, days_growth, year_growth = line.split("\t")
    err_handle = ticker, first_quot, last_quot, var, max_price, min_price, days_growth, year_growth

    try:

        if ticker not in results.keys():
            results[ticker] = []

        results[ticker].append(first_quot)
        results[ticker].append(last_quot)
        results[ticker].append(var)
        results[ticker].append(max_price)
        results[ticker].append(min_price)
        results[ticker].append(days_growth)
        results[ticker].append(year_growth)

    except ValueError as error:
        print(err_handle, error)

ordered_by_lastquot = sorted(results.items(), key=lambda kv: (kv[1][1], kv[0]))
ordered_by_lastquot.reverse()

# Final results print
for ticker in ordered_by_lastquot:
    print('%s\t%s\t%s\t%s\t%f\t%f\t%s\t%i' % (
        ticker[0], ticker[1][0], ticker[1][1], str(round(float(ticker[1][2]), 2)) + '%', round(float(ticker[1][3]), 2),
        round(float(ticker[1][4]), 2), ticker[1][5], int(ticker[1][6])))
