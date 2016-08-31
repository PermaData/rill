import re
import sys
import getopt

from .common import readwrite as io
from .common import definitions as d

__all__ = ['remove_chars']


@rill.component
@rill.inport('infile')
@rill.inport('outfile')
@rill.inport('chars')
def remove_chars(infile, outfile, chars, modified, substring=False, placeholder=''):
    """Remove all from a set of characters from a column.

    Input:
        chars:
        substring: If False (the default), chars is interpreted as a set
            of individual characters. If True, chars is interpreted as a
            defined substring, and this works like search and replace.
        placeholder: If '' (the default), the characters are removed.
            If another string, every character in chars is replaced by
            placeholder.
    """
    data = io.pull(infile, str)

    if (substring):
        # Treat chars as a strict substring
        target = re.escape(chars)
    else:
        # Treat chars as individual characters
        target = '[' + chars + ']'
    out = []
    for s in data:
        out.append(re.sub(target, placeholder, s).strip())
    io.push(out, outfile)


def parse_args(args):
    def help():
        print('remove_chars.py -i <input file> -o <output file> -c <characters to remove>')

    infile = None
    outfile = None
    chars = None

    options = ('i:o:c:',
               ['input', 'output', 'chars'])
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
            chars = value

    if (any(val is None for val in [infile, outfile, chars])):
        help()
        sys.exit(2)

    return infile, outfile, chars

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    remove_chars(*args)
