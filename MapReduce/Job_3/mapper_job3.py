#!/usr/bin/env python3
"""mapper.py"""

import sys
from datetime import datetime as dt

input_file = sys.stdin

# Not read the first line of input file
next(input_file)


# Read lines from input_file
for line in input_file:

	# Removing leading/trailing whitespaces
	line = line.strip()

	# Parse the input elements
	ticker, open_str, close_str, adj_close, low_str, high_str, volume, date_str = line.split(",")

	# Convert String to Date
	date_dt = dt.strptime(date_str, '%Y-%m-%d')

	if date_dt.year == 2017:

		print('%s\t%s\t%s' % (ticker, date_str, close_str))