""" String replace for column values. """
import csv
import getopt
import sys

import rill


@rill.component
@rill.inport('infile')
@rill.inport('outfile')
@rill.inport('to_replace')
@rill.inport('with_replace')
@rill.outport('modified')
def replace_text(infile, outfile, to_replace, with_replace, modified):
    """
    Replace text within field with new text.
    :param infile: input CSV file
    :param outfile: output CSV file
    :param to_replace: substring within field to be replaced
    :param with_replace: substring to replace to_replace substring
    """
    outfile_ = outfile.receive_once()
    ofile = open(outfile_, 'w')
    writer = csv.writer(ofile, delimiter=',', quoting=csv.QUOTE_NONE, lineterminator='\n')

    csv_data = []
    field_names = None
    to_replace_ = to_replace.receive_once()
    with_replace_ = with_replace.receive_once()
    with open(infile.receive_once()) as csv_values:
        reader = csv.reader(csv_values, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in reader:
            field = row[0].replace('\'', '')
            new_field = field.replace(to_replace_, with_replace_)
            row[0] = '\'' + new_field + '\''

            writer.writerow(row)
    ofile.close()
    modified.send(outfile_)


def parse_arguments(argv):
    """ Parse the command line arguments and return them. """
    infile = None
    outfile = None
    to_replace = None
    with_replace = None

    try:
        opts, args = getopt.getopt(argv, "hi:o:t:w:", ["infile=", "outfile=", "to_replace=", "with_replace="])
    except getopt.GetoptError:
        print('replace_text.py -i <GGD361 CSV file> -o <CSV output file> -t <text in field to replace> -w <replacement text>')
        sys.exit(2)

    found_in_file = False
    found_outfile = False
    found_to_replace = False
    found_with_replace = False
    for opt, arg in opts:
        if opt == '-h':
            print('replace_text.py -i <GGD361 CSV file> -o <CSV output file> -t <text in field to replace> -w <replacement text>')
            sys.exit()
        elif opt in ("-i", "--infile"):
            found_in_file = True
            infile = arg
        elif opt in ("-o", "--outfile"):
            found_outfile = True
            outfile = arg
        elif opt in ("-t", "--to_replace"):
            found_to_replace = True
            to_replace = arg
        elif opt in ("-w", "--with_replace"):
            found_with_replace = True
            with_replace = arg
    if not found_in_file:
        print("Input file '-i' argument required.")
        sys.exit(2)
    if not found_outfile:
        print("Output file '-o' argument required.")
        sys.exit(2)
    if not found_to_replace:
        print("Text within field to replace '-t' argument required.")
        sys.exit(2)
    if not found_with_replace:
        # print("Replacement text '-w' argument required.")
        # sys.exit(2)
        with_replace = ''
    return (infile, outfile, to_replace, with_replace)


if __name__ == '__main__':
    (infile, output_csv, to_replace, with_replace) = parse_arguments(sys.argv[1:])

    replace_text(infile.strip(), output_csv.strip(), to_replace.strip(), with_replace.strip())
