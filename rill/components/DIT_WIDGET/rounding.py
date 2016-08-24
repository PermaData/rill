import sys
import getopt
import math

from .common import readwrite as io
from .common import definitions as d

__all__ = ['rounding']


def rounding(infile, outfile, mode, precision=0):
    """Round values to the nearest integer.

    modes:
        up/ceil/ceiling: round to the next integer towards +inf.
        down/floor: round to the next integer towards -inf.
        trunc/truncate: truncate decimal part, rounding towards 0.
        nearest/round: round to the nearest integer. If precision is
            given, instead round to that many digits beyond the decimal
            point.
    """
    data = io.pull(infile, float)

    input_map = {'up': _ceil, 'ceil': _ceil, 'ceiling': _ceil,
                 'down': _floor, 'floor': _floor,
                 'trunc': _trunc, 'truncate': _trunc,
                 'nearest': _round, 'round': _round,
                 }

    conv = input_map[mode.lower()]

    out = [[conv(item, precision) for item in row] for row in data]

    io.push(out, outfile)


def _ceil(val, precision):
    return float(math.ceil(val * 10**precision)) / precision


def _floor(val, precision):
    return float(math.floor(val * 10**precision)) / precision


def _trunc(val, precision):
    return int(val)


def _round(val, precision):
    return round(val, precision)


def parse_args(args):
    def help():
        print('rounding.py -i <input file> -o <output file> -m <rounding mode> [-p <precision>]')

    infile = None
    outfile = None
    mode = None

    precision = 0

    options = ('i:o:m:p:', ['input', 'output', 'mode', 'precision'])
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
        elif (option in readoptions[2]):
            mode = value
        elif (option in readoptions[3]):
            precision = int(value)

    if (any(val is None for val in [infile, outfile, mode])):
        help()
        sys.exit(2)

    return infile, outfile, mode

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    rounding(*args)
