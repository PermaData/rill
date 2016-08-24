"""Converts latitude/longitude coordinates into UTM. The input coordinates must
be in decimal format."""
import csv
import utm
import sys
import getopt

__all__ = ['latlong_to_utm']


def latlong_to_utm(infile, outfile, lat_i, long_i, header=True):
    with open(infile, 'r', newline='') as original, \
         open(outfile, 'w', newline='') as output:
        data = csv.reader(original)
        push = csv.writer(output)
        for row in data:
            if (header):
                ENZL = ('East', 'North', 'Zone', '')
                header = False
            else:
                lat = float(row[lat_i])
                long = float(row[long_i])
                ENZL = utm.from_latlon(lat, long)
            push.writerow(modify_row(row, ENZL, lat_i, long_i))


def modify_row(row, ENZL, lat_i, long_i):
    out = [entry for (i, entry) in enumerate(row) if i not in (lat_i, long_i)]
    # Join the zone number and letter
    modified = ENZL[0:2] + (str(ENZL[2]) + ENZL[3],)
    out.extend(modified)
    return out


def parse_args(args):
    def help():
        print('latlong_to_utm.py -i <input CSV file> -o <output csv file> '
              '-t <latitude column index> -n <longitude column index>')

    infile = None
    outfile = None
    lat_col = None
    long_col = None

    hemisphere = 'north'

    options = ('i:o:t:n:',
               ['input', 'output', 'latitude_index', 'longitude_index'])
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

    if (any(val is None for val in [infile, outfile, lat_col, long_col])):
        help()
        sys.exit(2)

    return infile, outfile, lat_col, long_col

#                 PERFORM FUNCTION USING COMMAND-LINE OPTIONS                 #
if (__name__ == '__main__'):
    args = parse_args(sys.argv[1:])

    latlong_to_utm(*args)
