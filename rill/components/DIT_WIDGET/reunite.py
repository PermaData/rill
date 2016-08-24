import csv


def reunite(outfile, *args):
    """Unites all column files into a single csv.
    Inputs:
        outfile: The name of the csv file to write into.
        *args: Any number of filenames which hold individual columns.
            The columns will be put into the output file in the order
            they appear in this function call.
    Outputs:
        Writes into outfile.
    """
    with open(outfile, 'wb') as f:
        final = csv.writer(f)
        try:
            for (i, name) in enumerate(args):
                files[i] = open(args[i])

            for line in itertools.izip(*files):
                final.writerow(line)
        finally:
            close_all(files)
