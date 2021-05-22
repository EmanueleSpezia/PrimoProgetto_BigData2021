#!/usr/bin/env python3
"""reducer.py"""

import sys

# Initialize the local variables
ticker_result = None
sector_result = None

# Read lines from stdin
for line in sys.stdin:
    
    # Removing leading/trailing whitespaces
    line = line.strip()

    # Parse the input elements
    ticker, close_price_str, volume_str, date_str, sector = line.split('\t')

    # Put ticker and sector in memory to be print in final line with other information
    if sector != '-':
        ticker_result = ticker
        sector_result = sector
        continue
    
    print('%s\t%s\t%s\t%s\t%s' % (ticker_result, close_price_str, volume_str, date_str, sector_result))