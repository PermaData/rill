""" Reformats a file of one column of date/times into GTN-P specific date/time format. """

import csv
import datetime as dt
import getopt
import sys

gtnp_date_time_format = '%Y-%m-%d %H:%M'


def reformat_dates_to_gtnp(column_file, out_file, in_format):
    """
    Reformat the date/times.
    :param column_file: file containing date/time column
    :param out_file: CSV filename for reformatted date/times
    :param in_format: python strptime format string of date/times in column_file
    """
    date_time_writer = csv.writer(open(out_file, 'wb'), lineterminator='\n')
    with open(column_file, 'rb') as csvfile:
        date_time_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in date_time_reader:
            try:
                date_time = dt.datetime.strptime(row[0].strip(), in_format)
                quoted_dt = "{0}".format(date_time.strftime(gtnp_date_time_format))
                date_time_writer.writerow([quoted_dt])
            except ValueError as error:
                print(error)
                date_time_writer.writerow(row)


def parse_arguments(argv):
    """ Parse the command line arguments and return them. """
    date_time_column_file = None
    out_column_file = None
    date_time_format = None
    try:
        opts, args = getopt.getopt(argv, "hi:o:f:", ["date_time_column_file=", "out_column_file=", "date_time_format="])
    except getopt.GetoptError:
        print('reformat_dates_to_gtnp.py -i <Date time column file> -o <CSV output file> -f <date/time format>')
        sys.exit(2)

    found_in_file = False
    found_out_file = False
    found_date_time = False
    for opt, arg in opts:
        if opt == '-h':
            print('reformat_dates_to_gtnp.py -i <Date time column file> -o <CSV output file> -f <date/time format>')
            sys.exit()
        elif opt in ("-i", "--date_time_column_file"):
            found_in_file = True
            date_time_column_file = arg
        elif opt in ("-o", "--out_column_file"):
            found_out_file = True
            out_column_file = arg
        elif opt in ("-f", "--date_time_format"):
            found_date_time = True
            date_time_format = arg
    if not found_in_file:
        print("Input file '-i' argument required.")
        sys.exit(2)
    if not found_out_file:
        print("Output file '-o' argument required.")
        sys.exit(2)
    if not found_date_time:
        print("Input date/time format '-f' argument required.")
        sys.exit(2)
    return (date_time_column_file, out_column_file, date_time_format)

if __name__ == '__main__':
    (date_time_column_file, out_column_file, date_time_format) = parse_arguments(sys.argv[1:])

    reformat_dates_to_gtnp(column_file=date_time_column_file, out_file=out_column_file, in_format=date_time_format)
