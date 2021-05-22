#!/usr/bin/env python3
"""mapper.py"""

import sys
from datetime import datetime as dt

# Read lines from stdin
for line in sys.stdin:

    # Removing leading/trailing whitespaces
    line = line.strip()

    # Parse the input elements
    ticker, close_str, volume_str, date_str, sector = line.split('\t')

    # Convert String to Date
    date_dt = dt.strptime(date_str, '%Y-%m-%d')

    start_period = dt.strptime("2009-01-01", "%Y-%m-%d")
    end_period = dt.strptime("2018-12-31", "%Y-%m-%d")

    if (date_dt >= start_period) and (date_dt <= end_period) and sector != 'None' and sector != 'N/A':
        print('%s\t%s\t%s\t%s\t%s' % (ticker, date_str, close_str, volume_str, sector))
