""" For each row in code translation file:
    - extract relevant condition and value columns into a new csv file
    - kick off conditional replacement script
    This script assumes the input CSV file has only one value for the first condition.
    For example if the first condition is a value for station ID, each input CSV file
    is assumed to have only one station ID.
"""
import copy
import csv
import getopt
import sys
from . import replace_text
from . import move_text


def get_header_indices(condition_cols, replacement_cols, csv_header, are_moving):
    """ Get column indices to be put in new file by checking condition and
        replacement headers against csv header.
    """
    col_names = clean_list(csv_header)
    col_indices = []
    zipped_cols = copy.deepcopy(condition_cols)
    zipped_cols.extend(replacement_cols)
    if len(zipped_cols) != 3:
        if are_moving:
            print("3 columns are required: ID, moving from, moving to")
            sys.exit(2)
        else:
            print("3 columns are required: ID, to replace, replace with")
            sys.exit(2)
    if not are_moving and zipped_cols[1] != zipped_cols[2]:
        print("Replacing requires value to be replaced and a value to replace with for the same column.")
        sys.exit(2)
    if not are_moving:
        del zipped_cols[-1]
    for code in zipped_cols:
        try:
            col_indices.append(col_names.index(code))
        except ValueError:
            print("Cannot find column {0} in CSV file.".format(code))
            sys.exit(2)
    return col_indices


def column(matrix, i):
    return [row[i] for row in matrix]


def clean_value(value):
    new_val = value.strip().replace('\'', '')
    return new_val


def clean_list(list_to_clean):
    return [clean_value(value) for value in list_to_clean]


def split_list(list_to_split, split_at):
    cleaned_list = clean_list(list_to_split)
    split_ind = cleaned_list.index(split_at)
    before = cleaned_list[:split_ind]
    after = cleaned_list[split_ind + 1:]
    return before, after


def get_conditions_replacements(codes):
    with open(codes) as code_values:
        code_reader = csv.reader(code_values, delimiter=',', quoting=csv.QUOTE_NONE)
        header = code_reader.next()
        condition_cols, replacement_cols = split_list(header, '=')
        condition_replacements = [split_list(row, '=') for row in code_reader]
    return condition_cols, replacement_cols, condition_replacements


def subset_csv_data(csv_reader, col_ind, are_moving):
    """ Grab subset columns of CSV data to hand to replacement scripts. """
    csv_data = []
    full_data = []
    for row in csv_reader:
        new_row = []
        clean_row = clean_list(row)
        for ind in col_ind[1:]:
            new_row.append(clean_row[ind])
        csv_data.append(new_row)
        full_data.append(row)
    return csv_data, full_data


def subset_code_data(the_id, mapping):
    """ Grab subset of condition/replacement rows matching 'ID' (first column
        of CSV and condition file).
    """
    clean_id = clean_value(the_id)
    code_subset = []
    for conds, replaces in mapping:
        if conds[0] == clean_id:
            pair = (conds, replaces)
            code_subset.append(pair)

    return code_subset


def translate_codes(ggd361_csv, out_file, move_codes, replace_codes):
    """
    :param ggd361_csv: input CSV file
    :param out_file: output CSV file
    :param codes: codes CSV file
    """

    are_moving = move_codes is not None
    codes = move_codes
    if not are_moving:
        codes = replace_codes

    csv_data = []
    condition_cols, replacement_cols, mapping = get_conditions_replacements(codes)

    col_ind = None
    csv_header = None
    with open(ggd361_csv) as csv_values:
        reader = csv.reader(csv_values, delimiter=',', quoting=csv.QUOTE_NONE)
        csv_header = reader.next()
        col_ind = get_header_indices(condition_cols, replacement_cols, csv_header, are_moving)
        csv_subset, csv_full = subset_csv_data(reader, col_ind, are_moving)

    temp_in = 'temp_in.csv'
    temp_out = 'temp_out.csv'
    code_subset = subset_code_data(csv_full[0][0], mapping)

    # For each row of condition/replacement script call appropriate replacement script:
    for conditions, replacements in code_subset:
        with open(temp_in, 'w') as tfile:
            writer = csv.writer(tfile, delimiter=',', quoting=csv.QUOTE_NONE, lineterminator='\n')
            writer.writerows(csv_subset)
        if are_moving:
            # Call with to_replace (condition), with_replace (replacement), subset column file
            move_text.move_text(temp_in, temp_out, conditions[1], replacements[0])
        else:
            # Call with list of conditions, list of replacements, subset column file
            replace_text.replace_text(temp_in, temp_out, conditions[1], replacements[0])
        with open(temp_out, 'r') as tfile:
            reader = csv.reader(tfile, delimiter=',', quoting=csv.QUOTE_NONE)
            csv_subset = []
            for row in reader:
                csv_subset.append(row)

    # Read in output file from replacement script containing just columns to be replaced (one or more).
    with open(temp_out, 'r') as tfile:
        write_csv = csv_header
        for ind, full_row in enumerate(csv_full):
            replace_ind = 0
            for ind in col_ind[1:]:
                full_row[ind] = csv_subset[ind][replace_ind]
                replace_ind = replace_ind + 1
            write_csv.append(full_row)

    # Write out original CSV file with new column values.
    with open(out_file, 'w') as ofile:
        writer = csv.writer(ofile, delimiter=',', quoting=csv.QUOTE_NONE, lineterminator='\n')
        writer.writerows(write_csv)


def parse_arguments(argv):
    """ Parse the command line arguments and return them. """
    ggd361_csv = None
    out_file = None
    move_codes = None
    replace_codes = None

    try:
        opts, args = getopt.getopt(argv, "hi:o:m:r:", ["ggd361_csv=", "out_file=", "move_codes=", "replace_codes="])
    except getopt.GetoptError:
        print('replace_text.py -i <GGD361 CSV file> -o <CSV output file> -m <move codes CSV file> -r <replace codes CSV file>')
        sys.exit(2)

    found_in_file = False
    found_out_file = False
    found_move_codes = False
    found_replace_codes = False
    for opt, arg in opts:
        if opt == '-h':
            print('replace_text.py -i <GGD361 CSV file> -o <CSV output file> -d <depth code CSV file>')
            sys.exit()
        elif opt in ("-i", "--ggd361_csv"):
            found_in_file = True
            ggd361_csv = arg
        elif opt in ("-o", "--out_file"):
            found_out_file = True
            out_file = arg
        elif opt in ("-m", "--move_codes"):
            found_move_codes = True
            move_codes = arg
        elif opt in ("-r", "--r_codes"):
            found_replace_codes = True
            replace_codes = arg
    if not found_in_file:
        print("Input file '-i' argument required.")
        sys.exit(2)
    if not found_out_file:
        print("Output file '-o' argument required.")
        sys.exit(2)
    if not found_move_codes and not found_replace_codes:
        print("Either move or replace code translation file '-m' or '-r' argument required.")
        sys.exit(2)
    if found_move_codes and found_replace_codes:
        print("Either move or replace code translation file '-m' or '-r' argument required.")
        sys.exit(2)
    return (ggd361_csv, out_file, move_codes, replace_codes)


if __name__ == '__main__':
    (ggd361_csv, output_csv, move_codes, replace_codes) = parse_arguments(sys.argv[1:])

    translate_codes(ggd361_csv.strip(), output_csv.strip(), move_codes, replace_codes)
