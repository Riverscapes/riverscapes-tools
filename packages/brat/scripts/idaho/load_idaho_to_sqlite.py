import sqlite3
import os
import sys
import traceback
import argparse
from osgeo import ogr
from osgeo import osr
from rscommons.shapefile import load_attributes, load_geometries, get_transform_from_epsg
from rsxml import ProgressBar, dotenv
from rscommons import ModelConfig

fields = [
    'StreamName',
    'HUC_ID',
    'iGeo_ElMax',
    'iGeo_ElMin',
    'iGeo_Len',
    'iGeo_Slope',
    'iGeo_DA',
    'iVeg100EX',
    'iVeg_30EX',
    'iVeg100Hpe',
    'iVeg_30Hpe',
    'iPC_RoadX',
    'iPC_RoadVB',
    'iPC_Road',
    'iPC_RailVB',
    'iPC_Rail',
    'iPC_Canal',
    'iPC_DivPts',
    'ADMIN_AGEN',
    'iPC_Privat',
    'iPC_LU',
    'iPC_VLowLU',
    'iPC_LowLU',
    'iPC_ModLU',
    'iPC_HighLU',
    'oPC_Dist',
    'IsMainCh',
    'IsMultiCh',
    # 'Orig_DA',
    'iHyd_SP2',
    'iHyd_QLow',
    'iHyd_Q2',
    'iHyd_SPLow',
    'oVC_Hpe',
    'oVC_EX',
    'oCC_HPE',
    'mCC_HPE_CT',
    'oCC_EX',
    'mCC_EX_CT',
    'mCC_HisDep',
    'oPBRC_UI',
    'oPBRC_UD',
    'oPBRC_CR'
]


def load_idaho(shapefile, database):

    conn = sqlite3.connect(database)

    # Clear the database first
    conn.execute('DELETE FROM Reaches')
    conn.commit()

    lookup = {
        'oPBRC_CR': load_lookup(database, 'DamOpportunities', 'OpportunityID'),
        'oPBRC_UI': load_lookup(database, 'DamRisks', 'RiskID'),
        'oPBRC_UD': load_lookup(database, 'DamLimitations', 'LimitationID'),
        'ADMIN_AGEN': load_lookup(database, 'Agencies', 'AgencyID', 'Abbreviation')
    }

    db_fields = []
    for field in fields:
        if field == 'HUC_ID':
            db_fields.append('WatershedID')
        elif field == 'FCode':
            db_fields.append('ReachCode')
        elif field == 'ADMIN_AGEN':
            db_fields.append('AgencyID')
        elif field == 'oPBRC_CR':
            db_fields.append('OpportunityID')
        elif field == 'oPBRC_UI':
            db_fields.append('RiskID')
        elif field == 'oPBRC_UD':
            db_fields.append('LimitationID')
        else:
            db_fields.append(field)

    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataset = driver.Open(shapefile, 0)
    layer = dataset.GetLayer()
    sr_idaho = layer.GetSpatialRef()

    out_spatial_ref, transform = get_transform_from_epsg(sr_idaho, 4326)

    progbar = ProgressBar(layer.GetFeatureCount(), 50, "Loading features")
    counter = 0
    for feature in layer:
        counter += 1
        progbar.update(counter)

        geom = feature.GetGeometryRef()
        geom.Transform(transform)
        reach_values = [geom.ExportToJson()]

        for field in fields:
            if field in lookup:
                value = feature.GetField(field)

                if field == 'oPBRC_UI' and str.isdigit(value[0]):
                    value = value[1:]
                elif value == '4PVT':
                    value = 'PVT'

                reach_values.append(lookup[field][value])
            else:
                reach_values.append(feature.GetField(field))

        conn.execute('INSERT INTO Reaches (Geometry, {}) Values (?, {})'.format(','.join(db_fields), ','.join('?' * len(db_fields))), reach_values)

    conn.commit()
    progbar.finish()

    print('Process complete')


def load_lookup(database, table, id_field, name_field='Name'):

    conn = sqlite3.connect(database)
    curs = conn.cursor()
    curs.execute("SELECT {}, {} FROM {}".format(name_field, id_field, table))
    lookup = {}
    for row in curs.fetchall():
        lookup[row[0]] = row[1]

    return lookup


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('shapefile', help='Path to full Idaho ShapeFile', type=str)
    parser.add_argument('database', help='Output SQLite database (must exist already)', type=str)
    args = dotenv.parse_args_env(parser)

    load_idaho(args.shapefile, args.database)


if __name__ == '__main__':
    main()
