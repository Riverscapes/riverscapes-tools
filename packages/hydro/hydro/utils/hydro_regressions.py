import csv
import json
import datetime
import traceback
import sys
import argparse
import sqlite3
import numpy as np
from sklearn.linear_model import LinearRegression

from rsxml import Logger, dotenv, ProgressBar


def generate_linear_regressions(data_dict, x_key, y_key):
    """
    Generate linear regression models using values from a dictionary and compare
    a regular linear regression model to a log-transformed model, returning the better fit.

    Parameters:
    data_dict (dict): Dictionary containing the data.
    x_key (str): Key to be used as the independent variable.
    y_key (str): Key to be used as the dependent variable.

    Returns:
    dict: Dictionary containing the better model's coefficients, intercept, and R^2 score.
    """
    if len(data_dict[x_key]) != len(data_dict[y_key]):
        raise ValueError('The independent and dependent variables must have the same number of values.')
    if len(data_dict[x_key]) < 3:
        raise ValueError('At least 3 data points are required to generate a linear regression model.')

    # Extract data from the dictionary
    X = np.array(data_dict[x_key]).reshape(-1, 1)
    y = np.array(data_dict[y_key])
    # Find indices where X is 0
    zero_indices = np.where(X == 0)[0]

    # Remove these indices from X and y
    X = np.delete(X, zero_indices, axis=0)
    y = np.delete(y, zero_indices, axis=0)

    # Create and train the regular linear regression model
    model_regular = LinearRegression(fit_intercept=False)
    model_regular.fit(X, y)
    r2_regular = model_regular.score(X, y)

    # Create and train the log-transformed linear regression model
    X_log = np.log10(X)
    y_log = np.log10(y)
    model_log = LinearRegression()
    model_log.fit(X_log, y_log)
    r2_log = model_log.score(X_log, y_log)

    # Compare R^2 scores and return the better model
    if r2_regular >= r2_log:
        return {
            'model_type': 'regular',
            'coefficients': model_regular.coef_,
            'intercept': model_regular.intercept_,
            'r2_score': r2_regular
        }
    else:
        return {
            'model_type': 'log-transformed',
            'coefficients': model_log.coef_,
            'intercept': model_log.intercept_,
            'r2_score': r2_log
        }


def update_csv_rows(file_path, target_column, target_value, update_column, update_value):
    """
    Update rows in a CSV file where the target column matches the target value.

    Parameters:
    file_path (str): Path to the CSV file.
    target_column (str): Name of the column to be checked for the target value.
    target_value (str): The value to be matched in the target column.
    new_value (str): The new value to be set in the target column.

    Returns:
    None
    """
    # Read the CSV file
    with open(file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
        fieldnames = reader.fieldnames

    # Update the rows
    for row in rows:
        if row[target_column] == target_value:
            row[update_column] = update_value

    # Write the updated rows back to the CSV file
    with open(file_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_gage_data(db_path, huc, minimum_gages):

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()

    out_data = {'DA': [], 'Qlow': [], 'Q2': []}

    # select hucs whose hucs = huc
    curs.execute(f"SELECT da, min_discharge, peak_discharge FROM sites LEFT JOIN discharges on sites.site_no = discharges.site_no WHERE huc = '{huc}' and is_valid = 1")
    selection = curs.fetchall()
    selection = [gage for gage in selection if None not in gage]
    if len(selection) >= minimum_gages:
        for gage in selection:
            out_data['DA'].append(gage[0])
            out_data['Qlow'].append(gage[1])
            out_data['Q2'].append(gage[2])
        level = 8
        if None in out_data:
            print('here')
        return out_data, len(selection), level
    else:
        # step up to huc6 and add to select huc[0:6] = huc[0:6]
        curs.execute(f"SELECT da, min_discharge, peak_discharge FROM sites LEFT JOIN discharges on sites.site_no = discharges.site_no WHERE SUBSTR(huc, 0, 7) = '{huc[:6]}' and is_valid = 1")
        selection = curs.fetchall()
        selection = [gage for gage in selection if None not in gage]
        if len(selection) >= minimum_gages:
            for gage in selection:
                out_data['DA'].append(gage[0])
                out_data['Qlow'].append(gage[1])
                out_data['Q2'].append(gage[2])
            level = 6
            if None in out_data:
                print('here')
            return out_data, len(selection), level
        else:
            # step up to huc4 and add to selection huc[0:4] = huc[0:4]
            curs.execute(f"SELECT da, min_discharge, peak_discharge FROM sites LEFT JOIN discharges on sites.site_no = discharges.site_no WHERE SUBSTR(huc, 0, 5) = '{huc[:4]}' and is_valid = 1")
            selection = curs.fetchall()
            selection = [gage for gage in selection if None not in gage]
            if len(selection) >= minimum_gages:
                for gage in selection:
                    out_data['DA'].append(gage[0])
                    out_data['Qlow'].append(gage[1])
                    out_data['Q2'].append(gage[2])
                level = 4
                if None in out_data:
                    print('here')
                return out_data, len(selection), level
            else:
                # step up to huc2 and add to selection huc[0:2] = huc[0:2]
                curs.execute(f"SELECT da, min_discharge, peak_discharge FROM sites LEFT JOIN discharges on sites.site_no = discharges.site_no WHERE SUBSTR(huc, 0, 3) = '{huc[:2]}' and is_valid = 1")
                selection = curs.fetchall()
                selection = [gage for gage in selection if None not in gage]
                if len(selection) >= minimum_gages:
                    for gage in selection:
                        out_data['DA'].append(gage[0])
                        out_data['Qlow'].append(gage[1])
                        out_data['Q2'].append(gage[2])
                    level = 2
                    if None in out_data:
                        print('here')
                    return out_data, len(selection), level
                else:
                    raise ValueError(f"Insufficient gages for HUC {huc}")


def update_watersheds_table(csv_path, db_path, operator):

    log = Logger('Flow Equations')

    metadata = {}

    # get list of huc8s from watersheds table
    with open(csv_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        huc8s_qlow = [row['WatershedID'] for row in reader if row['Qlow'] == '']
        log.info(f"Updating {len(huc8s_qlow)} watersheds with missing Qlow values.")
    with open(csv_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        huc8s_q2 = [row['WatershedID'] for row in reader if row['Q2'] == '']
        log.info(f"Updating {len(huc8s_q2)} watersheds with missing Q2 values.")

    progbar = ProgressBar(len(huc8s_qlow), 50, 'Updating watersheds')
    counter = 0
    for huc in huc8s_qlow:
        try:
            data, gage_ct, huc_level = generate_gage_data(db_path, huc, 5)
            qlow = generate_linear_regressions(data, 'DA', 'Qlow')
            if huc in huc8s_q2:
                q2 = generate_linear_regressions(data, 'DA', 'Q2')
                huc8s_q2.remove(huc)
            else:
                q2_r2 = None

            if qlow['model_type'] == 'regular':
                qlow_eqn = f"{qlow['coefficients'][0]} * DRNAREA"
                qlow_r2 = qlow['r2_score']
            else:
                qlow_eqn = f"{10 ** qlow['intercept']} * DRNAREA ** {qlow['coefficients'][0]}"
                qlow_r2 = qlow['r2_score']
            if q2['model_type'] == 'regular':
                q2_eqn = f"{q2['coefficients'][0]} * DRNAREA"
                q2_r2 = q2['r2_score']
            else:
                q2_eqn = f"{10 ** q2['intercept']} * DRNAREA ** {q2['coefficients'][0]}"
                q2_r2 = q2['r2_score']

            metadata[huc] = {'Operator': operator, 'DateCreated': datetime.datetime.now().isoformat(), 'NumGages': gage_ct, 'HucLevel': huc_level, 'QLowR2': qlow_r2, 'Q2R2': q2_r2}

            update_csv_rows(csv_path, 'WatershedID', huc, 'Qlow', qlow_eqn)  # is it going to be slow doing this one by one?
            update_csv_rows(csv_path, 'WatershedID', huc, 'Q2', q2_eqn)
            update_csv_rows(csv_path, 'WatershedID', huc, 'Metadata', json.dumps(metadata[huc]))

        except ValueError as e:
            log.error(e)

        counter += 1
        progbar.update(counter)

    for huc in huc8s_q2:
        try:
            data, gage_ct, huc_level = generate_gage_data(db_path, huc, 5)
            q2 = generate_linear_regressions(data, 'DA', 'Q2')
            if q2['model_type'] == 'regular':
                q2_eqn = f"{q2['intercept']} + {q2['coefficients'][0]} * DRNAREA"
                q2_r2 = q2['r2_score']
            else:
                q2_eqn = f"{10 ** q2['intercept']} * DRNAREA ** {q2['coefficients'][0]}"
                q2_r2 = q2['r2_score']

            update_csv_rows(csv_path, 'WatershedID', huc, 'Q2', q2_eqn)
            if huc in metadata.keys():
                metadata[huc]['Q2R2'] = q2_r2
            else:
                metadata[huc] = {'Operator': operator, 'DateCreated': datetime.datetime.now().isoformat(), 'NumGages': gage_ct, 'HucLevel': huc_level, 'Q2R2': q2_r2}

            update_csv_rows(csv_path, 'WatershedID', huc, 'Q2', q2_eqn)
            update_csv_rows(csv_path, 'WatershedID', huc, 'Metadata', json.dumps(metadata[huc]))

        except ValueError as e:
            log.error(e)


def main():

    parser = argparse.ArgumentParser(description='Update the flow equations in the watersheds table.')
    parser.add_argument('csv_path', type=str, help='Path to the CSV file containing the watersheds table.')
    parser.add_argument('db_path', type=str, help='Path to the SQLite database containing the gages table.')
    parser.add_argument('operator', type=str, help='The person updating the flow equations.')
    parser.add_argument('--verbose', action='store_true', help='Print log messages to the console.', default=False)

    args = dotenv.parse_args_env(parser)

    log = Logger('Flow Equations')
    log.setup(log_path='', verbose=args.verbose)
    log.title('Update Flow Equations using USGS Gage Data')

    try:
        update_watersheds_table(args.csv_path, args.db_path, args.operator)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
