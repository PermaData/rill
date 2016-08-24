import sys
import getopt
import csv

from .common import readwrite as io
from .common import definitions as d

__all__ = ['remove_duplicate']


def remove_duplicate(infile, outfile):
    """Remove duplicate records from the data."""
    with open(infile) as fi:
        with open(outfile, 'w') as fo:
            data = csv.reader(fi, quoting=csv.QUOTE_NONNUMERIC, quotechar="'")
            push = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC,
                              lineterminator='\n', quotechar="'")

            track = set()
            duplicates = 0
            for row in data:
                test = tuple(row)
                if (test in track):
                    duplicates += 1
                else:
                    track.add(test)
                    push.writerow(row)
            # print('Found {0} duplicates').format(duplicates)


def quote(line):
    out = []
    for item in line:
        try:
            out.append(float(item))
        except ValueError:
            out.append("'" + item + "'")
    return out


def parse_args(args):
    def help():
        print('remove_duplicate.py -i <input file> -o <output file>')

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

    remove_duplicate(*args)
