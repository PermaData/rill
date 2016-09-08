def pull(infile, mode):
    """Read data from infile into a list of numbers.

    Inputs:
        infile: A string with the name of the file to read from.
        mode: A conversion function to ingest the data, either built-in
            like 'float' or your own.
    Outputs:
        A list of the data converted from infile.
    """
    out = []

    with open(infile) as f:
        for line in f:
            sub = line.split(',')
            for i, field in enumerate(sub):
                sub[i] = mode(field)
            if (len(sub) == 1):
                # Single value, you don't want it wrapped in a list
                out.append(sub[0])
            else:
                out.append(sub)
    return out


def push(data, outfile):
    """Write data from a list into outfile.

    Inputs:
        data: A one-dimensional list of string and/or numeric data.
        outfile: A string with the name of the file to write into.
    Outputs:
        No return value.
        Writes each element of data onto a new line. Numeric data is
            formatted with specified precision.
    """

    with open(outfile, mode='w') as f:
        for line in data:
            if (isinstance(line, str)):
                # Write strings directly on their own line
                f.write(line + '\n')
            elif (isinstance(line, int)):
                # Ints are plain to write
                f.write('{n}\n'.format(n=line))
            else:
                # Write decimals with 7 decimal points precision
                f.write('{n:0.{p}f}\n'.format(n=line, p=7))


# def log(data, outfile):
#     """Write data from a list into outfile.
#
#     Inputs:
#         data: A one-dimensional list of strings.
#         outfile: A string with the name of the file to write into.
#     Outputs:
#         No return value.
#         Writes the strings from data into outfile.
#     """
#
#     with open(outfile, mode='a') as f:
#         for line in data:
#             f.write(line)
#             f.write('\n')


# def disp(data):
#     """Print a list of data to the screen.
#
#     Inputs:
#         data: A one-dimensional list of objects to be printed.
#     Outputs:
#         None.
#     """
#
#     for line in data:
#         print line
