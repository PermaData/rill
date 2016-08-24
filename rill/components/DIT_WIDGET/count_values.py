"""Counts the number of unique values that occur in a column file OR
Given two columns, counts the number of unique values in column 2 that
    correspond to values in column 1."""

import sys
import getopt

from .common import readwrite as io
from .common import definitions as d

__all__ = ['count_values']


def count_values(infile, outfile, mode='single'):
    data = io.pull(infile, str)

    if (mode == 'single'):
        out = single(data)
    elif (mode == 'double'):
        out = double(data)

    io.push(out, outfile)


def single(data):
    """Count the number of unique values that occur in a data set.
    Inputs:
        data: A 1-dimensional list of values
    Outputs:
        out: The number of unique values collected
    """
    values = set()
    for item in data:
        values.add(item)
    out = [len(values)]
    return out


def double(data):
    """Count the number of unique values of one data set matched with another.
    Inputs:
        data: A list of 2-element lists that represent rows. The first
            one is assumed to be the primary.
    Outputs:
        out: A list of strings showing primary value: number of
            secondary values
    """
    values = {}  # Maps value: set of occurrences
    for first, second in data:
        if (first not in values and first not in d.missing_values):
            values[first] = set([second])
        else:
            values[first].add(second)
    out = [len(values)]
    for key in sorted(values.keys()):
        out.append('{0}: {1}'.format(key, len(values[key])))
    return out


def parse_args(args):
    def help():
        print('count_values.py -i <input file> -o <output file> [-m <mode>]')

    infile = None
    outfile = None

    mode = 'single'

    options = ('i:o:m:', ['input', 'output', 'mode'])
    readoptions = zip(['-'+c for c in options[0] if c != ':'],
                      ['--'+o for o in options[1]])

    try:
        (vals, extras) = getopt.getopt(args, *options)
    except getopt.GetoptError as e:
        print(str(e))
        help()
        sys.exit(2)

    for (option, value) in vals:
        if (option in readoptions[0]):
            infile = value
        elif (option in readoptions[1]):
            outfile = value
        elif (option in readoptions[2]):
            mode = value

    if (any(val is None for val in [infile, outfile])):
        help()
        sys.exit(2)

    return infile, outfile, mode

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    count_values(*args)
