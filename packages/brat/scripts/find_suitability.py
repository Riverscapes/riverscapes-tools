import csv
from dbfpy import dbf
import shutil
import os

# -------------------------------------------------------------------------------
# Name:        Find Suitability
# Purpose:     To aid in importing pyBRAT vegetation suitability values to sqlBRAT
#
# Author:      Tyler Hatch
#
# Created:     07/2020
# -------------------------------------------------------------------------------

# Step 3
region = 'Middle Rockies'

# Step 4
vegetation_types_csv = r''

# Step 5
py_brat_veg_dbf_existing = r''

# Step 6
py_brat_veg_dbf_historical = r''

# Step 10
exceptions = []


def main():

    # Get list of Google Sheets Data
    defaults_list = csv_to_list(vegetation_types_csv)

    # Get important indexes
    veg_col = defaults_list[0].index('VegetationID')
    suit_col = defaults_list[0].index('DefaultSuitability')
    name_col = defaults_list[0].index('Name')
    epoch_col = defaults_list[0].index('Epoch')

    # Remove column names
    defaults_list.pop(0)

    # Make a list that contains both vegetation rasters
    rasters_list = [py_brat_veg_dbf_historical, py_brat_veg_dbf_existing]

    # Initialize variables
    total_items = 0
    overrides = 0
    found = 0
    add_list = []
    mismatches = []
    override_text = []
    replacements = []
    previous_value = 0

    # For historic and existing
    for py_brat_veg_dbf in rasters_list:

        this_epoch = parse_epoch(py_brat_veg_dbf)

        # Copy new data
        new_dbf_copy = py_brat_veg_dbf.replace('.dbf', '[COPY].dbf')
        shutil.copyfile(py_brat_veg_dbf, new_dbf_copy)
        new_csv_copy = dbf_to_csv(new_dbf_copy)
        new_list = csv_to_list(new_csv_copy)

        # Get important indexes
        new_veg_col = new_list[0].index('VALUE')
        new_suit_col = new_list[0].index('VEG_CODE')
        if 'CLASSNAME' in new_list[0]:
            new_name_col = new_list[0].index('CLASSNAME')
        elif 'BPS_NAME' in new_list[0]:
            new_name_col = new_list[0].index('BPS_NAME')
        else:
            new_name_col = new_list[0].index('EVT_NAME')

        # Remove column names
        new_list.pop(0)

        # Increment item totals
        total_items += len(new_list)

        # Check each row in the pyBRAT vegetation list to see how it compares to the Google Sheet
        for new_row in new_list:

            # This is to keep track of whether or not the item was found in the Google Sheet
            found_it = False

            # Check every row in the Google Sheets until we find a match (Inefficient, but still runs fine)
            for default_row in defaults_list:

                # Do we need to convert between EVT 1.4.0 and 2.0.0?
                if 'EVT' in default_row[epoch_col] and '140' in py_brat_veg_dbf:
                    conversion = 4000
                else:
                    conversion = 0

                # We found a matching vegetation ID
                if str(int(new_row[new_veg_col]) + conversion) == default_row[veg_col]:

                    # Increment Counters
                    found_it = True
                    found += 1

                    # Do the names match?
                    if new_row[new_name_col] != default_row[name_col] and int(default_row[veg_col]) not in exceptions:
                        # The names are a mismatch
                        mismatches.append([default_row[veg_col], new_row[new_name_col], default_row[name_col]])

                    elif new_row[new_suit_col] != default_row[suit_col] and default_row[suit_col] == '0':
                        # The names are a match, but the suitabilities are not, and we're overriding a zero

                        # Formatting Outputs
                        if (previous_value + 1 == int(new_row[new_veg_col])) or (previous_value == 0):
                            spacer = ""
                        else:
                            spacer = "\n\t"
                        replacements.append([spacer + str(int(new_row[new_veg_col]) + conversion), new_row[new_suit_col]])
                        previous_value = int(new_row[new_veg_col])

                    elif new_row[new_suit_col] != default_row[suit_col]:
                        # The names are a match, but the suitabilities are not
                        override_text.append("{}\t{} {} {}\t{}".format(region, default_row[veg_col], default_row[epoch_col], default_row[name_col], new_row[new_suit_col]))
                        overrides += 1

                    break

            if not found_it:
                # Could not find that specific value in the Google Sheet
                add_list.append([str(int(new_row[new_veg_col]) + conversion), new_row[new_name_col], this_epoch, new_row[new_suit_col]])

        # Remove duplicate files
        for file in [new_dbf_copy, new_csv_copy]:
            os.remove(file)

    if len(replacements) > 0:
        bar()
        print('Present in Google Sheet, but default suitability should not be zero')
        bar()

        for replacement in replacements:
            print('\t{} should have a suitability of {}'.format(replacement[0], replacement[1]))

    if len(override_text) > 0:
        bar()
        print("Present in Google Sheet, but suitabilities mismatch")
        bar()

        for override in override_text:
            print(override)

    if len(mismatches) > 0:
        bar()
        print("Present in Google Sheet, but names mismatch")
        bar()

        for mismatch in mismatches:
            print("{}\t[{}] / [{}]".format(mismatch[0], mismatch[1], mismatch[2]))

    if len(add_list) > 0:
        bar()
        print("Not Present in Google Sheet")
        bar()

        for item in add_list:
            print("{}\t{}\t{}\t{}".format(item[0], item[1], item[2], item[3]))

    bar()
    print("{}/{} were overrides.".format(overrides, total_items))
    print("{}/{} were found.".format(found, total_items))


def dbf_to_csv(filename):

    if filename.endswith('.dbf'):
        csv_fn = filename[:-4] + ".csv"
        with open(csv_fn, 'wb') as csvfile:
            in_db = dbf.Dbf(filename)
            out_csv = csv.writer(csvfile)
            names = []
            for field in in_db.header.fields:
                names.append(field.name)
            out_csv.writerow(names)
            for rec in in_db:
                out_csv.writerow(rec.fieldData)
            in_db.close()
            return filename.replace('.dbf', '.csv')
    else:
        print("Filename does not end with .dbf")


def csv_to_list(csv_to_read):
    data = []
    with open(csv_to_read, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for line in reader:
            data.append(line)

    return data


def parse_epoch(file):

    if 'evt' in file:
        return 'Landfire 2.0.0 EVT'
    if 'bps' in file:
        return 'Landfire 2.0.0 BPS'
    else:
        return 'Unknown Epoch'


def bar():
    print("------------------------------------------------------------")


if __name__ == '__main__':
    main()
