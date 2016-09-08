import re
import sys
import getopt

import rill

from .common import readwrite as io
from .common import definitions as d


@rill.component
@rill.inport('infile')
@rill.inport('outfile')
@rill.inport('chars')
def remove_chars(infile, outfile, chars):
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
    # Move to multiple recieves later
    _infile = infile.receive_once()
    _outfile = outfile.receive_once()
    _chars = chars.receive_once()
    # Used to be optional arguments, and may be so again if I can figure that out
    substring = False
    placeholder = ''

    data = io.pull(_infile, str)

    if (substring):
        # Treat chars as a strict substring
        target = re.escape(_chars)
    else:
        # Treat chars as individual characters
        target = '[' + _chars + ']'
    out = []
    for s in data:
        out.append(re.sub(target, placeholder, s).strip())
    io.push(out, _outfile)
