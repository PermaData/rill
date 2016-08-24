#!/usr/bin/python
import sys
import getopt

from .common import readwrite as io
from .common import definitions as d

__all__ = ['decimal_to_minsec']


def decimal_to_minsec(infile, outfile):
    f = open(infile)
    data = f.readlines()

    out = [['', ''] for all in range(len(data))]
    formatstr = '{deg:03.0f}\xb0 {min:02.0f}\' {sec:02.0f}" {hemi}'
    negatives = ['S', 'W']
    positives = ['N', 'E']
    for row, pair in enumerate(data):
        coordinates = standardize(pair.strip())
        for col, coord in enumerate(coordinates):
            degree = int(coord)
            rm = abs(coord) % 1
            minute = int(rm * 60)
            rm = (rm * 60) % 1
            second = rm * 60
            out[row][col] = formatstr.format(deg=abs(degree), min=minute,
                    sec=second, hemi=negatives[col] if degree < 0 else
                    positives[col])

    io.push(interpret_out(out), outfile)


def standardize(coordstring):
    """Creates a tuple from a standard decimal coordinate string."""
    out = coordstring.replace('\xb0', '').replace('W', '*-1').replace('S', '*-1')
    out = out.replace('N', '').replace('E', '')
    return eval(out)


def interpret_out(data):
    out = []
    for line in data:
        out.append('{0}, {1}'.format(line[0], line[1]))
    return out


def parse_args(args):
    def help():
        print('minsec_to_decimal.py -i <input file> -o <output file>')

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

    if (any(val is None for val in [infile, outfile])):
        help()
        sys.exit(2)

    return infile, outfile

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    decimal_to_minsec(*args)
