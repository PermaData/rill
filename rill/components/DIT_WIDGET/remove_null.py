import sys
import getopt
import csv

from .common import readwrite as io
from .common import definitions as d

__all__ = ['remove_null']


def remove_null(infile, outfile):
    """Remove records with no data from the dataset."""
    with open(infile) as fi:
        with open(outfile, 'w') as fo:
            data = csv.reader(fi, quoting=csv.QUOTE_NONNUMERIC, quotechar="'")
            push = csv.writer(fo, quoting=csv.QUOTE_NONNUMERIC,
                              lineterminator='\n', quotechar="'")

            for row in data:
                keep = False
                for item in row:
                    try:
                        test = float(item)
                    except ValueError:
                        test = item
                    if (test not in d.missing_values):
                        keep = True
                        break
                if (keep):
                    push.writerow(row)


def parse_args(args):
    def help():
        print('remove_null.py -i <input file> -o <output file>')

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

    remove_null(*args)
