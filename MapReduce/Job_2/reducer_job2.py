#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime as dt

sector_ticker_list = {}
volume_year = {}

beginning_year = {}  # Example: {'APPL': {2010: [2010-01-02, 115.40], 2011: [2011-01-12, 12.45]}}
ending_year = {}

results = {}

# Read lines from stdin
for line in sys.stdin:

    # Removing leading/trailing whitespaces
    line = line.strip()

    # Parse the input elements
    ticker, date_str, close_str, volume_str, sector = line.split('\t')

    # Convert String to Date
    date_dt = dt.strptime(date_str, '%Y-%m-%d')
    close = float(close_str)
    volume = int(volume_str)

    # List of tickers for every sector
    if sector not in sector_ticker_list:
        sector_ticker_list[sector] = []
        sector_ticker_list[sector].append(ticker)
    else:
        sector_ticker_list[sector].append(ticker)

    # Calculate the entire volume of tickers
    if ticker not in volume_year:
        volume_year[ticker] = {}
        volume_year[ticker][date_dt.year] = volume
    elif date_dt.year not in volume_year[ticker].keys():
        volume_year[ticker][date_dt.year] = volume
    else:
        volume_year[ticker][date_dt.year] += volume

    # Calculate for every ticker, the close price of beginning of years between 2009-2018
    if ticker not in beginning_year:
        beginning_year[ticker] = {}
        beginning_year[ticker][date_dt.year] = []
        beginning_year[ticker][date_dt.year].append(date_dt)
        beginning_year[ticker][date_dt.year].append(close)

    elif date_dt.year not in beginning_year[ticker].keys():
        beginning_year[ticker][date_dt.year] = []
        beginning_year[ticker][date_dt.year].append(date_dt)
        beginning_year[ticker][date_dt.year].append(close)

    elif date_dt < beginning_year[ticker][date_dt.year][0]:
        del beginning_year[ticker][date_dt.year][0]
        del beginning_year[ticker][date_dt.year][0]

        beginning_year[ticker][date_dt.year].append(date_dt)
        beginning_year[ticker][date_dt.year].append(close)

    # Calculate for every ticker, the close price of ending of years between 2009-2018
    if ticker not in ending_year:
        ending_year[ticker] = {}
        ending_year[ticker][date_dt.year] = []
        ending_year[ticker][date_dt.year].append(date_dt)
        ending_year[ticker][date_dt.year].append(close)

    elif date_dt.year not in ending_year[ticker].keys():
        ending_year[ticker][date_dt.year] = []
        ending_year[ticker][date_dt.year].append(date_dt)
        ending_year[ticker][date_dt.year].append(close)

    elif date_dt > ending_year[ticker][date_dt.year][0]:
        del ending_year[ticker][date_dt.year][0]
        del ending_year[ticker][date_dt.year][0]

        ending_year[ticker][date_dt.year].append(date_dt)
        ending_year[ticker][date_dt.year].append(close)

for sector in sector_ticker_list.keys():
    results[sector] = {}

    for yr in range(2009, 2019, 1):
        total_quotation_begin = 0
        total_quotation_end = 0
        best_ticker = None
        best_ticker_volume = None

        for ticker in sector_ticker_list[sector]:

            if (yr in beginning_year[ticker].keys()) and (yr in ending_year[ticker].keys()):
                # Point a
                total_quotation_begin += beginning_year[ticker][yr][1]
                total_quotation_end += ending_year[ticker][yr][1]

            if (yr in beginning_year[ticker].keys()) and (yr in ending_year[ticker].keys()):

                # Point b
                if best_ticker is None:
                    best_ticker = ticker
                else:
                    if (round(((ending_year[ticker][yr][1] - beginning_year[ticker][yr][1]) /
                               beginning_year[ticker][yr][1]) * 100)) > (round(((ending_year[best_ticker][yr][1] -
                                                                                 beginning_year[best_ticker][yr][1]) /
                                                                                beginning_year[best_ticker][yr][
                                                                                    1]) * 100)):
                        best_ticker = ticker

            if yr in volume_year[ticker].keys():
                # Point c
                if best_ticker_volume is None:
                    best_ticker_volume = ticker
                else:
                    if volume_year[ticker][yr] > volume_year[best_ticker_volume][yr]:
                        best_ticker_volume = ticker

        results[sector][yr] = []
        results[sector][yr].append(
            round(((total_quotation_end - total_quotation_begin) / total_quotation_begin) * 100, 2))
        results[sector][yr].append(best_ticker)
        results[sector][yr].append((round(((ending_year[best_ticker][yr][1] - beginning_year[best_ticker][yr][1]) /
                                           beginning_year[best_ticker][yr][1]) * 100, 2)))
        results[sector][yr].append(best_ticker_volume)
        results[sector][yr].append(volume_year[best_ticker_volume][yr])

sorted_by_sector = sorted(results, key=lambda k: k[0], reverse=True)

for el in sorted_by_sector:
    for i in range(2018, 2008, -1):
        print('%s\t%i\t%s\t%s\t%s\t%s\t%i' % (
            el, i, ("{:.2f}".format(results[el][i][0])), results[el][i][1], ("{:.2f}".format(results[el][i][2])),
            results[el][i][3], results[el][i][4]))
