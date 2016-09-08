"""Counts the number of valid (non-empty) records in a column file."""

import sys
import getopt

import rill

from .common import readwrite as io
from .common import definitions as d



def count_records(infile, outfile):
    """Count how many valid records there are."""
    data = io.pull(infile, float)

    out = 0
    for val in data:
        if (val not in d.missing_values):
            out += 1

    io.push([out], outfile)


#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    count_records(*args)
