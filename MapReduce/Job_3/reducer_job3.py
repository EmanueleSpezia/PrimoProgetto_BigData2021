#!/usr/bin/env python3
"""reducer.py"""

import sys
from datetime import datetime as dt

first_close = {}
last_close = {}
ticker_month_var = {}
ignore_ticker = {}

# Read lines from stdin
for line in sys.stdin:

    # Removing leading/trailing whitespaces
    line = line.strip()

    # Parse the input elements
    ticker, date_str, close_str = line.split('\t')

    # Convert String to Date
    date_dt = dt.strptime(date_str, '%Y-%m-%d')
    close = float(close_str)

    # Found the first day of every month with the close price
    if ticker not in first_close:
        first_close[ticker] = {}
        first_close[ticker][date_dt.month] = []
        first_close[ticker][date_dt.month].append(date_dt.day)
        first_close[ticker][date_dt.month].append(close)

    elif date_dt.month not in first_close[ticker].keys():
        first_close[ticker][date_dt.month] = []
        first_close[ticker][date_dt.month].append(date_dt.day)
        first_close[ticker][date_dt.month].append(close)

    elif date_dt.day < first_close[ticker][date_dt.month][0]:
        del first_close[ticker][date_dt.month][0]
        del first_close[ticker][date_dt.month][0]

        first_close[ticker][date_dt.month].append(date_dt.day)
        first_close[ticker][date_dt.month].append(close)

    # Found the last day of every month with the close price
    if ticker not in last_close:
        last_close[ticker] = {}
        last_close[ticker][date_dt.month] = []
        last_close[ticker][date_dt.month].append(date_dt.day)
        last_close[ticker][date_dt.month].append(close)

    elif date_dt.month not in last_close[ticker].keys():
        last_close[ticker][date_dt.month] = []
        last_close[ticker][date_dt.month].append(date_dt.day)
        last_close[ticker][date_dt.month].append(close)

    elif date_dt.day > last_close[ticker][date_dt.month][0]:
        del last_close[ticker][date_dt.month][0]
        del last_close[ticker][date_dt.month][0]

        last_close[ticker][date_dt.month].append(date_dt.day)
        last_close[ticker][date_dt.month].append(close)

# Filling ticker_month_var dictionary with ticker and percentage variation for every month
for ticker in first_close.keys():
    for i in range(1, 13):
        if i in last_close[ticker].keys():
            if ticker not in ticker_month_var:
                ticker_month_var[ticker] = {}
                ticker_month_var[ticker][i] = round(
                    ((last_close[ticker][i][1] - first_close[ticker][i][1]) / first_close[ticker][i][1]) * 100, 2)

            else:
                ticker_month_var[ticker][i] = round(
                    ((last_close[ticker][i][1] - first_close[ticker][i][1]) / first_close[ticker][i][1]) * 100, 2)

# Print the ticker couple witch percentage variation are similar (threshold = 3%)
couple_number = 0
for ticker in ticker_month_var:
    ignore_ticker[ticker] = ticker
    for ticker_compare in ticker_month_var:
        count = 0
        if (ticker_compare != ticker) and (ticker_compare not in ignore_ticker.keys()):
            for m in range(1, 13):
                if (m in ticker_month_var[ticker].keys()) and (m in ticker_month_var[ticker_compare].keys()):
                    if (0 <= (ticker_month_var[ticker][m] - ticker_month_var[ticker_compare][m]) <= 3) or (
                            -3 <= (ticker_month_var[ticker][m] - ticker_month_var[ticker_compare][m]) <= 0):
                        count += 1

                    if count == 12:
                        couple_number += 1

                        print('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
                        'Soglia: 3%', 'Coppia ' + str(couple_number) + ':{' + ticker + ',' + ticker_compare + '}: ',
                        'GEN: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][1])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][1])) + '%',
                        'FEB: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][2])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][2])) + '%',
                        'MAR: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][3])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][3])) + '%',
                        'APR: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][4])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][4])) + '%',
                        'MAG: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][5])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][5])) + '%',
                        'GIU: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][6])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][6])) + '%',
                        'LUG: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][7])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][7])) + '%',
                        'AGO: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][8])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][8])) + '%',
                        'SET: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][9])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][9])) + '%',
                        'OTT: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][10])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][10])) + '%',
                        'NOV: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][11])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][11])) + '%',
                        'DEC: ' + ticker + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker][12])) + '%, ' + ticker_compare + ' ' + (
                            "{:.2f}".format(ticker_month_var[ticker_compare][12])) + '%'))
