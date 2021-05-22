#!/usr/bin/env python3
"""mapper.py"""

import sys

input_file = sys.stdin

# Not read the first line of input file
next(input_file)

# Read lines from input_file
for line in input_file:

    # Removing leading/trailing whitespaces
    line = line.strip()

    # Parse the input elements
    words = line.split(',')

    if len(words) == 8:
        # Row of historical_stock_prices.csv
        ticker, open_price_str, close_price_str, adj_close, low_str, high_str, volume_str, date_str = words

        print('%s\t%s\t%s\t%s\t-' % (ticker, close_price_str, volume_str, date_str))
    else:
        # Row of historical_stocks.csv
        if len(words) == 5:
            ticker, exchange, name, sector, industry = words

            print('%s\t-\t-\t-\t%s' % (ticker, sector))
