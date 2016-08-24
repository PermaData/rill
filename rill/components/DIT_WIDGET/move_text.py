""" String move for column values. Move in this case means when the pattern
    exists in the indicated column:
    1) removing the pattern from that column
    2) adding the pattern to the other column as indicated
    This script assumes it is receiving a CSV file with 2 columns, the column
    from which to remove and the column to which to add.
"""
import csv
import getopt
import re
import sys


def move_text(ggd361_csv, output_csv, move_from_regex, move_to_regex):
    """
    move text within field with new text.
    :param ggd361_csv: input CSV file
    :param out_file: output CSV file
    :param move_from_regex: substring regular expression to move
    :param move_to_regex: substring regular expression to replace
    """
    ofile = open(output_csv, 'w')
    writer = csv.writer(ofile, delimiter=',', quoting=csv.QUOTE_NONE, lineterminator='\n')

    with open(ggd361_csv) as csv_values:
        reader = csv.reader(csv_values, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in reader:
            if is_a_match(move_from_regex, row):
                new_row = move_pattern(move_from_regex, move_to_regex, row)
                writer.writerow(new_row)
            else:
                writer.writerow(row)
    ofile.close()


def clean_value(value):
    new_value = value.strip().replace('\'', '')
    return new_value


def move_pattern(move_from_regex, move_to_regex, row):
    new_row = []
    for ind, value in enumerate(row):
        new_value = clean_value(value)
        matches = re.match(move_from_regex, clean_value(row[0]))
        if matches and ind == 0:
            new_value = '0'
            if len(matches.groups()) >= 1:
                new_value = matches.group(1)
        elif matches:
            new_value = move_to_regex.replace('.*', clean_value(value))
        new_row.append('\'' + new_value + '\'')
    return new_row


def is_a_match(move_from_regex, row):
    matches = re.match(move_from_regex, clean_value(row[0]))
    return matches is not None


def parse_arguments(argv):
    """ Parse the command line arguments and return them. """
    ggd361_csv = None
    out_file = None
    move_from_regex = None
    move_to_regex = None

    try:
        opts, args = getopt.getopt(argv, "hi:o:f:t:", ["ggd361_csv=", "out_file=", "move_from_regex=", 'move_to_regex='])
    except getopt.GetoptError:
        print('move_text.py -i <GGD361 CSV file> -o <CSV output file> -f <move from column regular expression> -t <move to column position expression>')
        sys.exit(2)

    found_in_file = False
    found_out_file = False
    found_move_from_regex = False
    found_move_to_regex = False
    for opt, arg in opts:
        if opt == '-h':
            print('move_text.py -i <GGD361 CSV file> -o <CSV output file> -t  <list of condition values> -w <list of movement values if conditions match>')
            sys.exit()
        elif opt in ("-i", "--ggd361_csv"):
            found_in_file = True
            ggd361_csv = clean_value(arg)
        elif opt in ("-o", "--out_file"):
            found_out_file = True
            out_file = clean_value(arg)
        elif opt in ("-f", "--move_from_regex"):
            found_move_from_regex = True
            move_from_regex = clean_value(arg)
        elif opt in ("-t", "--move_to_regex"):
            found_move_to_regex = True
            move_to_regex = clean_value(arg)
    if not found_in_file:
        print("Input file '-i' argument required.")
        sys.exit(2)
    if not found_out_file:
        print("Output file '-o' argument required.")
        sys.exit(2)
    if not found_move_from_regex:
        print("Regular expression of pattern to move from column '-e' argument required.")
        sys.exit(2)
    if not found_move_to_regex:
        print("Placement pattern of pattern to move in to column '-t' argument required.")
        sys.exit(2)
    return (ggd361_csv, out_file, move_from_regex, move_to_regex)


if __name__ == '__main__':
    (ggd361_csv, output_csv, move_from_regex, move_to_regex) = parse_arguments(sys.argv[1:])

    move_text(ggd361_csv, output_csv, move_from_regex, move_to_regex)
