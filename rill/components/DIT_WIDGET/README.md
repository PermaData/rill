# Widgets

Each of these scripts, hereafter termed "widgets", perform operations on the data given. They are meant to be chained together to perform more complex routines.  
They can be classified into several categories:


## Reading
#### These widgets read in the initial data and make it ready for processing.
- break_columns
- map_read


## Checking
#### These widgets verify the data in some way, showing some check values and allowing the user to inspect them.
- check_int
- count_values
- count_records
- pdf
- print_ge
- print_gt
- print_le
- print_lt
- print_rangex
- print_notin_rangex
- print_minmax
- statistics


## Manipulation
#### These widgets change data, altering them to suit the user's needs. They make up the majority of all the widgets.
- add_const
- sub_const
- mult_const
- div_const
- replace_eq
- replace_ge
- replace_gt
- replace_le
- replace_lt
- replace_rangex
- replace_notin_rangex
- decimal_to_minsec
- minsec_to_decimal
- find_tz
- latlong_to_utm
- utm_to_latlong
- mid_month
- move_text
- replace_text
- remove_chars
- remove_null
- remove_duplicate
- reformat_dates_to_gtnp
- rounding
- sort_by_columns
- translate_codes


## Writing
#### These widgets collect the results of all manipulations and write an output file for later use.
- create_gtnp_metadata_json
- reunite


# Developing new widgets

It is necessary that all widgets are in individual files, and that the main function of the widget has the same name as the file (minus the .py extension).
