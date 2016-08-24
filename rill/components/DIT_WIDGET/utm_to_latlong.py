#! /usr/bin/python

import csv
import utm
import string
import sys
import getopt

__all__ = ['utm_to_latlong']


def utm_to_latlong(infile, outfile, zone_i, E_i, N_i, zone_letter_i=None,
                   hemisphere='', header=True):
    """Convert values in a csv file from UTM coordinates to latitude/longitude.
    Inputs:
        infile: Name of CSV file that holds the UTM data.
        outfile: Name of CSV file to write recalculated data to
        zone_i: column index in infile with the zone data
        E_i: column index in infile with the easting data
        N_i: column index in infile with the northing data
        zone_letter_i: column index in infile with the zone letters
            (if applicable)
        hemisphere:
    Output:
        Writes
    """

    with open(infile, 'r', newline='') as original, \
         open(outfile, 'w', newline='') as output:
        data = csv.reader(original)
        push = csv.writer(output)
        for row in data:
            if (header):
                latlong = ('Lat', 'Lon')
                header = False
            else:
                if (zone_letter_i is not None):
                    zoneletter = row[zone_letter_i]
                    northern = None
                elif (row[zone_i][-1] in string.ascii_letters):
                    zoneletter = row[zone_i][-1]
                    zone = int(row[zone_i][:-1])
                    northern = None
                else:
                    zone = int(row[zone_i])
                    northern = hemisphere.lower() in ('n', 'north', 'northern')
                    zoneletter = None
                east = float(row[E_i])
                north = float(row[N_i])

                latlong = utm.to_latlon(east, north, zone, northern=northern,
                                        zone_letter=zoneletter)

            push.writerow(modify_row(row, latlong, zone_i, E_i, N_i))


def modify_row(row, latlong, zone_i, E_i, N_i):
    out = [entry for i, entry in enumerate(row) if i not in (zone_i, E_i, N_i)]
    out.extend(latlong)
    return out


def parse_args(args):
    def help():
        print('utm_to_latlong.py -i <input CSV file> -o <output csv file> -z '
              '<zone column index> -e <easting column index> -n '
              '<northing column index> [-L <zone letter column index>] '
              '[-h <hemisphere>]')

    # Required arguments
    infile = None
    outfile = None
    zone_col = None
    east_col = None
    north_col = None

    # Optional arguments
    hemisphere = 'north'
    zone_letter_i = None

    options = ('i:o:z:e:n:L:h:',
               ['input', 'output', 'zone_index', 'easting_index',
                'northing_index', 'zone_letter_index', 'hemisphere'])
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
            zone_col = int(value)
        elif (option in readoptions[3]):
            east_col = int(value)
        elif (option in readoptions[4]):
            north_col = int(value)
        elif (option in readoptions[5]):
            zone_letter_i = value
        elif (option in readoptions[6]):
            hemisphere = value

    if (any(val is None for val in
            [infile, outfile, zone_col, east_col, north_col])):
        help()
        sys.exit(2)

    return infile, outfile, zone_col, east_col, north_col, zone_letter_i, hemisphere

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    utm_to_latlong(*args)
