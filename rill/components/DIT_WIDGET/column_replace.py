import rill


def column_replace(base_csv, base_map, temp_csv, temp_map, base_map_updated):
    """
    name_map is the map created by column_extract (this should just be linked directly, bypassing the intervening widget)
    column_name is the name to be replaced
    data is what to replace the column with
    filename is the
    """
    """
    for name in temp_map:
        if name not in base_map:
            newcol = max(base_map.items()) + 1
            base_map[name] = newcol
    for mainline, templine in zip(base_csv, temp_csv):
        for name in temp_map:
            index = base_map[name]
            mainline[index] = templine[index]
        base_out_csv.writerow(mainline)
    """
