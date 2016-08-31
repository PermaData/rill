"""LOOKING AHEAD: This module splits an input csv file into individual
columns for easier manipulation by widgets."""

import csv
import itertools
import os


@rill.component
@rill.inport('datafile')
@rill.outport('outfiles')
@rill.inport('header')
@rill.inport('name_override')
def break_columns(datafile, outfiles, header, name_override):
    """
    Inputs:
        datafile: The name of a csv file that is to be read.
        header: If True (the default), the first line of the file is a
            header. If False, there is only data in the file.
        name_override: If None (the default), names are auto-selected
            by looking at the column headers or their position within
            the file. If a sequence, items that are strings will replace
            the names of corresponding columns (the first item will be
            the name of the first column, etc...) and items that are
            None or nonexistent will allow the auto-selection methods
            to run.
    Outputs:
        Writes each column of the data to a different file. The names of
        these files will be either the column headers (if they exist) or
        their zero-indexed position in the file.

        For FILE=
        Station,Date,Temperature
        XXX123,1999-03-21,-4.30265

        For FILE2=
        XYZ987,1492-12-12,3.5

        shard(FILE) -> Station.column, Date.column, Temperature.column
        shard(FILE2, header=False) -> 0.column, 1.column, 2.column
        shard(FILE, name_override=('st', 'dt', 'temp')) ->
            st.column, dt.column, temp.column
        shard(FILE, name_override=(None, 'DateTime')) ->
            Station.column, DateTime.column, Temperature.column
    """
    with open(datafile, 'r', newline='') as f:
        data = csv.reader(f, quoting=csv.QUOTE_NONNUMERIC, quotechar="'")
        column_check(data)
        f.seek(0)

        # Determine filenames
        firstline = next(data)
        if (name_override is not None):
            # Use the names given instead of finding them
            # If there are too few names in name_override
            name = ['' for all in firstline]
            for i in range(len(firstline)):
                if (i < len(name_override) and name_override[i]):
                    # If there is a name given for this position...
                    name[i] = name_override[i]
                else:
                    if (header):
                        name[i] = firstline[i]
                    else:
                        name[i] = i
        elif (header):
            # Use the values found in the header as column file names
            name = firstline
        else:
            # Use column indices as names
            name = list(range(len(firstline)))
            f.seek(0)  # Return to the top of the file to read again

        # Open files
        filenames = ['./DIT_WIDGET/RILL_TEST/columns/' + str(item) + '.column'
                     for item in name]
        files = [open(f, 'w') for f in filenames]

        # Write columns to files
        try:
            for line in data:
                for (i, item) in enumerate(line):
                    files[i].write(str(item) + '\n')
        finally:
            close_all(files)
    return filenames


def column_check(data):
    # TODO: Check if this exhausts the iterator in containing scope
    # NOTE: It does, you need to reset with a seek(0) command
    """Expects a csv.reader object."""
    count = []
    for line in data:
        count.append(len(line))
    mean = round(float(sum(count)) / len(count))
    error = False
    for (i, item) in enumerate(count):
        if (item != mean):
            print('Line {0} has a different number of columns than the rest '
                  'of the file.'.format(i + 1))
            error = True
    if (error):
        raise IndexError('One or more of the lines was flawed.')
    else:
        return True


def close_all(files):
    for opened in files:
        opened.close()
