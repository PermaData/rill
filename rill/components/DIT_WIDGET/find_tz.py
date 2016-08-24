"""Finds the time zone given latitude and longitude coordinates. If local date
and time are given, account for DST as well."""
# -*- coding: utf-8 -*-
import sys
import datetime
import time
import csv
import getopt

from tzwhere import tzwhere
import pytz

from .common import readwrite as io

__all__ = ['find_tz']


def find_tz(infile, outfile, dt_i, lat_i, lon_i, header=True):
    """
    Inputs:
        infile: name of input csv file.
        outfile: name of output csv file.
        dt_i: column index of date/time data. It must be in GTN-P
            standard format.
        lat_i: column index of latitude data. It must be in the form
            of a signed decimal number, where North is positive.
        lon_i: column index of longitude data. It must be in the form
            of a signed decimal number, where East is positive.
    """

    with open(infile, 'r') as input:
        with open(outfile, 'w') as output:
            data = csv.reader(input)
            push = csv.writer(output)

            finder = tzwhere.tzwhere()
            for line in data:
                if (header):
                    push.writerow(line+['UTC Offset'])
                    header = False
                else:
                    coord = (float(line[lat_i]), float(line[lon_i]))
                    tzname = finder.tzNameAt(*coord)
                    tm = time.strptime(line[dt_i].strip(), '%Y-%m-%d %H:%M')
                    dt = datetime.datetime(tm.tm_year, tm.tm_mon, tm.tm_mday,
                                           tm.tm_hour, tm.tm_min)
                    offset = name_to_offset(tzname, dt)

                    push.writerow(line + [offset])


def name_to_offset(name, dt):
    """Uses the pytz library to find the local offset from UTC."""
    tz = pytz.timezone(name)
    actual = tz.localize(dt)
    offset = actual.strftime('%z')
    return int(offset) * 36 / 60 / 60   # Returns offset in possibly fractional hours


def parse_args(args):
    def help():
        print('find_tz.py -i <input file> -o <output file> -d <date column index> -t <latitude column index> -n <longitude column index>')

    infile = None
    outfile = None
    dt_i = None
    lat_i = None
    lon_i = None

    options = ('i:o:d:t:n:',
               ['input', 'output', 'date_column_index', 'latitude_column_index', 'longitude_column_index'])
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
        elif (option in readoptions[2]):
            dt_i = int(value)
        elif (option in readoptions[2]):
            lat_i = int(value)
        elif (option in readoptions[2]):
            lon_i = int(value)

    if (any(val is None for val in [infile, outfile, dt_i, lat_i, lon_i])):
        help()
        sys.exit(2)

    return infile, outfile, dt_i, lat_i, lon_i

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    find_tz(*args)
