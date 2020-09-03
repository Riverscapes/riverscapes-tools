
from sqlbrat.lib.database import load_attributes


def hydrology_validation(idaho_db, brat_db):

    # Load the input fields required as well as the pyBRAT3 output fields
    feature_values = load_attributes(paths['Network'], 'ReachID', [veg_fis_field, 'iGeo_Slope', 'iGeo_DA', 'iHyd_SP2', 'iHyd_SPLow', 'iGeo_Len'])
    expected_output = load_attributes(paths['Network'], 'ReachID', [com_capacity_field, com_density_field])

    # Do the combined FIS calculation
    calculate_combined_fis(feature_values, veg_fis_field, com_capacity_field, com_density_field, max_drainage_area)

    # Merge the results into a master list
    for reach, feature in feature_values.items():
        capacity_values.append((expected_output[reach][com_capacity_field], feature[com_capacity_field]))
        density_values.append((expected_output[reach][com_density_field], feature[com_density_field]))

    # Plot the master list
    validation_chart(capacity_values, '{} Combined FIS Capacity'.format(label))
    validation_chart(density_values, '{} Combined FIS Density'.format(label))
