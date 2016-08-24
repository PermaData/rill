#! /usr/bin/python
import re
import sys
import getopt

from .common import readwrite as io
from .common import definitions as d

__all__ = ['minsec_to_decimal']


def minsec_to_decimal(infile, outfile):
    """Convert lat/long coordinates from minutes and seconds to decimal."""

    data = io.pull(infile, str)

    out = []
    for coord in data:
        # Splits each coordinate pair into degrees, minutes, seconds, and
        # hemisphere marker.
        coord = ','.join(coord)
        coord = coord.upper()
        subs = re.split(r'\s*[\xb0"\',]\s*|.(?=[NESW])|(?<=[NESW]).|\n', coord)
        subs = tuple(filter(None, subs))

        names = ['degrees', 'minutes', 'seconds']
        values = dict([(name, 0) for name in names])
        pair = [0, 0]
        for (which, section) in enumerate([subs[:len(subs) // 2],
                                           subs[len(subs) // 2:]]):
            sign = 1
            for (i, elem) in enumerate(section):
                if (elem in 'NESW'):
                    sign = -1 if elem in 'SW' else 1
                else:
                    values[names[i]] = float(elem)
            pair[which] = (values['degrees'] + values['minutes'] / 60
                           + values['seconds'] / 3600) * sign
        out.append(pair)

    io.push(interpret_out(out), outfile)


def interpret_out(data):
    out = []
    for line in data:
        out.append('{0:2.7f}, {1:3.7f}'.format(line[0], line[1]))
    return out


def parse_args(args):
    def help():
        print('minsec_to_decimal.py -i <input file> -o <output file>')

    infile = None
    outfile = None

    options = ('i:o:',
               ['input', 'output'])
    readoptions = zip(['-' + c for c in options[0] if c != ':'],
                      ['--' + o for o in options[1]])

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

    if (any(val is None for val in [infile, outfile])):
        help()
        sys.exit(2)

    return infile, outfile

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    minsec_to_decimal(*args)
