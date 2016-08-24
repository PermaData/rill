#! /usr/bin/python

import math
import sys
import getopt

from .common import readwrite as io
from .common import definitions as d

__all__ = ['statistics']


def statistics(infile, outfile):
    """Calculate and print statistical values of the data."""
    data = io.pull(infile, float)

    filtered = [x for x in data if x not in d.missing_values]

    point_stats = count_points(data)
    distribution = mean_std(filtered)
    minmax = min_max(filtered)

    out = []
    names = ['Min', 'Max', 'Mean', 'Standard Deviation', 'Total points',
             'Valid points', 'Valid fraction']
    formatstr = '{0}: {1:0.{p}f}'
    for name, value in zip(names, minmax + distribution + point_stats):
        out.append(formatstr.format(name, value, p=7))

    io.push(out, outfile)


# Helper functions

#   This one is passed the unfiltered data

def count_points(rawdata):
    total = 0
    valid = 0
    for point in rawdata:
        total += 1
        if(point not in d.missing_values):
            valid += 1

    return (total, valid, float(valid) / total)


#   These are passed the filtered data


def mean_std(data):
    """Calculates the mean and standard deviation of the given data set."""
    # Calculate the mean
    if(len(data) == 0):
        return (0, 0)
    else:
        mean = sum(data) / len(data)
    variance = 0
    for val in data:
        # Calculate the variance (the sum of squares of residuals)
        variance += (val - mean)**2
    # Take the square root to get standard deviation
    if(len(data) <= 1):
        return (mean, 0)
    else:
        std = math.sqrt(variance / (len(data)-1))
    return (mean, std)


def min_max(data):
    return (min(data), max(data))


def median(data):
    data.sort()
    if(len(data) % 2 == 1):
        # Return the center of the sorted list
        return data[len(data) // 2 + 1]
    else:
        # Return the mean of the two center values
        return (data[len(data) // 2] + data[len(data) // 2 + 1]) / 2


def parse_args(args):
    def help():
        print('statistics.py -i <input CSV file> -o <output csv file>')


    infile = None
    outfile = None

    options = ('i:o:',
                ['input', 'output'])
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

    if (any(val is None for val in
            [infile, outfile])):
        help()
        sys.exit(2)

    return infile, outfile

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    infile, outfile = parse_args(sys.argv[1:])

    statistics(infile, outfile)
