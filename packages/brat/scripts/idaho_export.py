# Philip Bailey
# 6 Nov 2019
# One-time script to merge Idaho BRAT regions into a single ShapeFile
# to be used to drive the BRAT Web Map
import os
from osgeo import ogr
from osgeo import osr

from rscommons.shapefile import load_attributes, load_geometries

risks = {
    'Negligible Risk': 1,
    'Some Risk': 2,
    'Minor Risk': 3,
    'Considerable Risk': 4
}

opportunities = {
    'Easiest - Low-Hanging Fruit': 1,
    'Straight Forward - Quick Return': 2,
    'Strategic - Long-Term Investment': 3,
    'NA': 4
}

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

top_level_folder = os.environ['SOME_PATH']
output = os.path.join(top_level_folder, 'idaho_brat_merge.shp')

regions = [
    'Clearwater_Region',
    'Magic_Valley_Region',
    'Panhandle_Region',
    'Salmon_Region',
    'Southeast_Region',
    'Southwest_Region',
    'Upper_Snake_Region'
]

# Create the output shapefile
driver = ogr.GetDriverByName('ESRI Shapefile')
if os.path.isfile(output):
    driver.DeleteDataSource(output)

# create the spatial reference, WGS84
outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(4326)
outDataSource = driver.CreateDataSource(output)
outLayer = outDataSource.CreateLayer('network', outSpatialRef, geom_type=ogr.wkbMultiLineString)
outLayer.CreateField(ogr.FieldDefn("ReachID", ogr.OFTInteger64))
outLayer.CreateField(ogr.FieldDefn("oCC_EX", ogr.OFTReal))
outLayer.CreateField(ogr.FieldDefn("oCC_HPE", ogr.OFTReal))
outLayer.CreateField(ogr.FieldDefn("oPBRC_UI", ogr.OFTInteger))
outLayer.CreateField(ogr.FieldDefn("oPBRC_CR", ogr.OFTInteger))

hucDefn = ogr.FieldDefn("HUC", ogr.OFTString)
hucDefn.SetWidth(8)
outLayer.CreateField(hucDefn)
outLayerDefn = outLayer.GetLayerDefn()

for region in regions:
    print('Processing', region)

    # Load conservation attributes first. That's all we need from this ShapeFile
    conserve = os.path.join(top_level_folder, region, '01_Perennial_Network/03_Conservation_Restoration_Model/Conservation_Restoration_Model_Perennial_{}.shp'.format(region.replace('_', '')))
    consatts = load_attributes(conserve, 'ReachID', ['oPBRC_UI', 'oPBRC_CR'])

    # Use the geometries from the capacity ShapeFile for the output
    capacity = os.path.join(top_level_folder, region, '01_Perennial_Network/02_Combined_Capacity_Model/Combined_Capacity_Model_Perennial_{}.shp'.format(region.replace('_', '')))
    inDataSource = driver.Open(capacity, 0)
    inLayer = inDataSource.GetLayer()
    transform = osr.CoordinateTransformation(inLayer.GetSpatialRef(), outSpatialRef)

    for feature in inLayer:
        reachid = feature.GetField('ReachID')
        geom = feature.GetGeometryRef()
        geom.Transform(transform)

        # Create output Feature
        outFeature = ogr.Feature(outLayerDefn)
        outFeature.SetField('ReachID', reachid)
        outFeature.SetField('oCC_EX', feature.GetField('oCC_EX'))
        outFeature.SetField('oCC_HPE', feature.GetField('oCC_HPE'))
        outFeature.SetField('HUC', feature.GetField('HUC_ID'))
        outFeature.SetField('oPBRC_UI', risks[consatts[reachid]['oPBRC_UI']])
        outFeature.SetField('oPBRC_CR', opportunities[consatts[reachid]['oPBRC_CR']])
        outFeature.SetGeometry(geom)
        outLayer.CreateFeature(outFeature)

print('Process complete')
