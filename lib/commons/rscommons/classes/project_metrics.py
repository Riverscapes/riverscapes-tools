from code import interact
import xml.etree.ElementTree as ET
import numpy as np
import argparse
import os
import json
import rasterio
from datetime import datetime
from rasterio.mask import mask
from osgeo import ogr
from rscommons.classes.raster import get_raster_cell_area, categorical_raster_count
from rscommons.classes.vector_base import get_utm_zone_epsg
from rscommons.raster_buffer_stats import raster_buffer_stats2
from rscommons.get_project_datasets import get_project_datasets
from rscommons import VectorBase, get_shp_or_gpkg
from rsxml import Logger, dotenv
from sympy import arg


class MetricsJson:

    def __init__(self, hydro_gpkg: str, project_xml: str):
        """This class creates a json file containing metrics summarized for model runs

        Args:
            HUC8 (str): path to a HUC8 WBD shapefile
            HUC12 (str): path to HUC12 WBD shapefile corresponding to the HUC8 shapefile entered
            project_xml (str): path to the project.rs.xml file for the project to be summarized
        """

        self.version = '0.0.2'

        self.log = Logger('Metrics')

        # will need dict to define which attributes to summarize
        self.config = {
            'vector': {
                'NHDFlowline': [
                    {'field': 'FCode'}
                ],
                'NHDArea': [
                    {'field': 'FCode'}
                ],
                'NHDWaterbody': [
                    {'field': 'FCode'}
                ],
                'flowlines': [
                    {'field': 'FCode'}
                ],
                'flowareas': [
                    {'field': 'FCode'}
                ],
                'waterbody': [
                    {'field': 'FCode'}
                ],
                'waterbody_filtered': [
                    {'field': 'FCode'}
                ],
                'flowarea_filtered': [
                    {'field': 'FCode'}
                ],
                'difference_polygons': [
                    {'field': 'FCode'}
                ],
                'channel_area': [
                    {'field': 'FCode'}
                ],
                'Ownership': [
                    {'field': 'ADMIN_AGEN'}
                ],
                'Ecoregions': [
                    {'field': 'NA_L3NAME'}
                ],
                'Roads': [
                    {'field': 'TNMFRC'}
                ],
                'BRAT_RESULTS': [
                    {
                        'field': 'oCC_EX',
                        'bins': [
                            {'label': 'None', 'lower': 0, 'upper': 0.01},
                            {'label': 'Rare', 'lower': 0.01, 'upper': 1},
                            {'label': 'Occasional', 'lower': 1, 'upper': 5},
                            {'label': 'Frequent', 'lower': 5, 'upper': 15},
                            {'label': 'Pervasive', 'lower': 15}
                        ]
                    },
                    {
                        'field': 'oCC_HPE',
                        'bins': [
                            {'label': 'None', 'lower': 0, 'upper': 0.01},
                            {'label': 'Rare', 'lower': 0.01, 'upper': 1},
                            {'label': 'Occasional', 'lower': 1, 'upper': 5},
                            {'label': 'Frequent', 'lower': 5, 'upper': 15},
                            {'label': 'Pervasive', 'lower': 15}
                        ]
                    },
                    {
                        'field': 'Opportunity'
                    },
                    {
                        'field': 'Risk'
                    },
                    {
                        'field': 'Limitation'
                    }
                ]
            },
            'raster': {
                'SLOPE': {
                    'bins': [
                        [{'label': '0-5', 'lower': 0, 'upper': 5},
                         {'label': '5-10', 'lower': 5, 'upper': 10},
                         {'label': '10-20', 'lower': 10, 'upper': 20},
                         {'label': '20-30', 'lower': 20, 'upper': 30},
                         {'label': '>30', 'lower': 30}]
                    ]
                }
            }
        }

        # parse project xml to get important info
        self.log.info('parsing project xml')
        self.project_xml = project_xml

        self.tree = ET.parse(self.project_xml)
        self.root = self.tree.getroot()

        self.project_type = self.root.find('ProjectType').text

        metadata = {}
        xmlmetadata = self.root.find('MetaData')
        meta = xmlmetadata.findall('Meta')
        for md in meta:
            metadata[md.attrib['name']] = md.text

        # Set up initial json
        self.log.info('setting up json file')
        self.metrics = {
            'project': {
                'projectType': self.project_type,
                'metricToolVersion': self.version,
                'metricToolRunTime': datetime.utcnow().isoformat(),
                'name': None,
                'area': None,
                'metaData': metadata,
                'metrics': {
                    'raster': {
                        'floatingPoint': [],
                        'categorical': []
                    },
                    'vector': {
                        'polygon': [],
                        'polyline': [],
                        'point': []
                    }
                },
                'huc12': {}
            }
        }

        # get dict for huc8 and huc12 where key=hucid and value=shapely object
        self.log.info('generating huc boundary data')
        self.huc8_polygons = {}
        self.huc12_polygons = {}  # maybe combine into a single dict?

        src = ogr.GetDriverByName('GPKG').Open(hydro_gpkg)

        h8lyr = src.GetLayer('WBDHU8')
        h8feature = list(h8lyr)
        self.huc8_polygons[h8feature[0].GetField('HUC8')] = VectorBase.ogr2shapely(h8feature[0].GetGeometryRef())

        h12lyr = src.GetLayer('WBDHU12')
        h12feature = list(h12lyr)
        for _, feature in enumerate(h12feature):
            self.huc12_polygons[feature.GetField('HUC12')] = VectorBase.ogr2shapely(feature.GetGeometryRef())

        # update metrics dict
        self.metrics['project']['name'] = h8feature[0].GetField('Name')
        self.metrics['project']['area'] = h8feature[0].GetField('AreaSqKm') * 1000000
        for _, feature in enumerate(h12feature):
            self.metrics['project']['huc12'][feature.GetField('HUC12')] = {'name': feature.GetField('Name'), 'area': feature.GetField('AreaSqKm') * 1000000, 'metrics':
                                                                           {'raster':
                                                                            {'floatingPoint': [], 'categorical': []},
                                                                            'vector':
                                                                            {'polygon': [], 'polyline': [], 'point': []}}}

        # find the epsg for transforms
        longitude = h8feature[0].GetGeometryRef().GetEnvelope()[0]
        self.epsg = get_utm_zone_epsg(longitude)

        # get list of each type of dataset and its name/id
        self.log.info('retrieving project datasets')
        self.float_datasets, self.cat_datasets, self.vector_datasets = get_project_datasets(self.project_xml)
        # change relative to full paths
        for i, ds in enumerate(self.float_datasets):
            self.float_datasets[i] = [os.path.join(os.path.dirname(self.project_xml), ds[0]), ds[1]]
        for i, ds in enumerate(self.cat_datasets):
            self.cat_datasets[i] = [os.path.join(os.path.dirname(self.project_xml), ds[0]), ds[1]]
        for i, ds in enumerate(self.vector_datasets):
            self.vector_datasets[i] = [os.path.join(os.path.dirname(self.project_xml), ds[0]), ds[1]]

        # clip polygons initially to huc8 to avoid long clip times for large polys
        # right now it's just Ownership and Ecoregions, may need to add others later if other datasets are added
        srs = h8lyr.GetSpatialRef()
        for i, ds in enumerate(self.vector_datasets):
            if ds[1] == 'Ownership':
                in_ds = ogr.GetDriverByName('ESRI Shapefile').Open(ds[0])
                in_lyr = in_ds.GetLayer()
                outDataSource = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(os.path.join(os.path.join(os.path.dirname(self.project_xml), ds[0])))
                outLayer = outDataSource.CreateLayer('clipped', srs, geom_type=ogr.wkbMultiPolygon)
                ogr.Layer.Clip(in_lyr, h8lyr, outLayer)
            elif ds[1] == 'Ecoregions':
                in_ds = ogr.GetDriverByName('ESRI Shapefile').Open(ds[0])
                in_lyr = in_ds.GetLayer()
                outDataSource = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(os.path.join(os.path.join(os.path.dirname(self.project_xml), ds[0])))
                outLayer = outDataSource.CreateLayer('clipped', srs, geom_type=ogr.wkbMultiPolygon)
                ogr.Layer.Clip(in_lyr, h8lyr, outLayer)

    def categorical_raster_metrics(self, dataset_name: str, raster_path: str, polygons: dict):
        """Calculates metrics for a categorical raster dataset in the project and adds them to the output json file.

        Args:
            dataset_name (str): layer id from the project.rs.xml
            raster_path (str): path to the raster
            polygons (dict): a dictionary of  the form {huc id: shapely geometry}
        """

        cell_area = get_raster_cell_area(raster_path)

        cats = {}
        for key in polygons.keys():
            cats[key] = {}

        with rasterio.open(raster_path) as src:

            for poly_id, polygon in polygons.items():

                raw_raster, _out_transform = mask(src, [polygon], crop=True)
                # mask_raster = np.ma.masked_values(raw_raster, src.nodata)

                for val in np.unique(raw_raster):
                    if val != src.nodata:
                        cats[poly_id][str(val)] = {'area': np.count_nonzero(raw_raster == val) * cell_area,
                                                   'count': np.count_nonzero(raw_raster == val)}

        if len(polygons) == 1:  # assumes this is for the huc8
            self.metrics['project']['metrics']['raster']['categorical'].append({dataset_name: {'cellSize': np.sqrt(cell_area), 'categories': cats[list(cats.keys())[0]]}})
        elif len(polygons) > 1:
            for key in cats.keys():
                self.metrics['project']['huc12'][key]['metrics']['raster']['categorical'].append({dataset_name: {'cellSize': np.sqrt(cell_area), 'categories': cats[key]}})

    def rasters_stats_to_metrics(self, raster_path: str, raster_stats_dict: dict, dataset_name: str):
        """Takes a dictionary of raster stats (from the raster buffer stats function output) and formats it to add to
        the project metrics json file.

        Args:
            raster_path (str): path to the raster
            raster_stats_dict (dict): a dictionary containing raster statistics from the function raster_buffer_stats2
            dataset_name (str): the layer id for the raster from the project.rs.xml file
        """

        cell_area = get_raster_cell_area(raster_path)

        if len(raster_stats_dict) == 1:
            self.metrics['project']['metrics']['raster']['floatingPoint'].append({dataset_name: {'cellSize': np.sqrt(cell_area),
                                                                                                 'max': raster_stats_dict[list(raster_stats_dict.keys())[0]]['Maximum'],
                                                                                                 'min': raster_stats_dict[list(raster_stats_dict.keys())[0]]['Minimum'],
                                                                                                 'avg': raster_stats_dict[list(raster_stats_dict.keys())[0]]['Mean'],
                                                                                                 'count': raster_stats_dict[list(raster_stats_dict.keys())[0]]['Count'],
                                                                                                 'sum': raster_stats_dict[list(raster_stats_dict.keys())[0]]['Sum']}})

        elif len(raster_stats_dict) > 1:
            for key in raster_stats_dict.keys():
                self.metrics['project']['huc12'][key]['metrics']['raster']['floatingPoint'].append({dataset_name: {'cellSize': np.sqrt(cell_area),
                                                                                                                   'max': raster_stats_dict[key]['Maximum'],
                                                                                                                   'min': raster_stats_dict[key]['Minimum'],
                                                                                                                   'avg': raster_stats_dict[key]['Mean'],
                                                                                                                   'count': raster_stats_dict[key]['Count'],
                                                                                                                   'sum': raster_stats_dict[key]['Sum']}})

    def binned_stats(self, raster_path: str, raster_id: str, polygons: dict, bins: list) -> dict:
        """Calculates metrics for continuous rasters separated into bins as specified in the self.config dictionary
        and adds the binned stats to the json output file

        Args:
            raster_path (str): path to the continous raster 
            raster_id (str): the layer id from the project.rs.xml file
            polygons (dict): dictionary of form {huc id: shapely geometry}
            bins (list): a list of dictionaries specifying upper and lower values for each bin; pulled from the 
            self.config dictionary

        Returns:
            dict: a dictionary containing the binned stats that is incorporated into the output json file
        """

        stats = {raster_id: {}}
        for _, poly in enumerate(polygons):
            stats[raster_id][str(poly)] = {}

        with rasterio.open(raster_path) as src:

            for polygon_id, polygon in polygons.items():

                raw_raster, _out_transform = mask(src, [polygon], crop=True)
                flat = raw_raster.flatten()
                flat = flat[(flat != src.nodata)]

                counts = {}

                for binnum, binl1 in enumerate(bins):
                    counts['bins{}'.format(str(binnum))] = []
                    for _, binn in enumerate(binl1):
                        if 'lower' not in binn.keys():
                            arr = flat[(flat < binn['upper'])]
                            counts['bins{}'.format(str(binnum))].append([None, binn['upper'], len(arr)])
                        elif 'upper' not in binn.keys():
                            arr = flat[(flat >= binn['lower'])]
                            counts['bins{}'.format(str(binnum))].append([binn['lower'], None, len(arr)])
                        else:
                            arr = flat[(flat >= binn['lower']) & (flat < binn['upper'])]
                            counts['bins{}'.format(str(binnum))].append([binn['lower'], binn['upper'], len(arr)])

                    # counts[]['total'] = len(flat)

                    stats[raster_id][polygon_id] = counts

        return stats

    def vector_metrics(self, vector_path: str, vector_id: str, polygons: dict, epsg: int) -> dict:
        """Calculates metrics for the vector layers within a project

        Args:
            vector_path (str): path to the vector layer
            vector_id (str): layer id from the project.rs.xml file
            polygons (dict): dictionary of form {huc id: shapely geometry}
            epsg (int): the epsg spatial reference number to use for transformations (for calculating areas and lengths)

        Returns:
            dict: a dictionary containing the vector metrics which is incorporated into the output json file
        """

        stats = {'point': {vector_id: {}}, 'polyline': {vector_id: {}}, 'polygon': {vector_id: {}}}

        for polyid, polygon in polygons.items():
            self.log.info('within huc: {}'.format(polyid))

            with get_shp_or_gpkg(vector_path) as lyr:
                sref, transform = VectorBase.get_transform_from_epsg(lyr.spatial_ref, epsg)
                # POINTS
                if lyr.ogr_geom_type in [1, 4, 1001, 1004, 2001, 2004, 3001, 3004, -2147483647, -2147483644]:
                    stats['point'][vector_id].update({polyid: {}})
                    if vector_id in self.config['vector'].keys():  # if there's a configuration specification
                        stats['point'][vector_id][polyid] = {'fields': {}}
                        for _, attr in enumerate(self.config['vector'][vector_id]):
                            if 'bins' in attr.keys():  # if there's bins
                                tot = 0
                                bins = attr['bins']
                                substats = {bins[i]['label']: 0 for i, bin in enumerate(bins)}
                                for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                                    field_val = feat.GetField(attr['field'])
                                    for i in range(len(bins)):
                                        if 'lower' not in bins[i].keys():
                                            if field_val < bins[i]['upper']:
                                                substats[bins[i]['label']] += 1
                                                tot += 1
                                        elif 'upper' not in bins[i].keys():
                                            if field_val >= bins[i]['lower']:
                                                substats[bins[i]['label']] += 1
                                                tot += 1
                                        else:
                                            if bins[i]['lower'] <= field_val < bins[i]['upper']:
                                                substats[bins[i]['label']] += 1
                                                tot += 1

                                stats['point'][vector_id][polyid]['fields'].update({attr['field']: substats})
                                stats['point'][vector_id][polyid].update({'countTotal': tot})

                            else:  # if separation is categorical
                                values_list = []  # get all unique values for field in config
                                tot = []  # for total number of points
                                for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                                    tot.append(fid)
                                    if feat.GetField(attr['field']) not in values_list:
                                        values_list.append(feat.GetField(attr['field']))
                                substats = {str(value): None for value in values_list}
                                for value in values_list:
                                    pts = []
                                    attribute_filter = '{0} = {1}'.format(attr['field'], value)
                                    for feat, fid, _ in lyr.iterate_features(attribute_filter=attribute_filter, clip_shape=polygon):
                                        pts.append(feat)
                                    substats[str(value)] = len(pts)

                                stats['point'][vector_id][polyid]['fields'].update({attr['field']: substats})
                                stats['point'][vector_id][polyid].update({'countTotal': len(tot)})

                    else:  # if there's not a config specification just count total points
                        pts = []
                        for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                            pts.append(feat)
                        stats['point'][vector_id][polyid] = {'countTotal': len(pts)}

                # LINES
                elif lyr.ogr_geom_type in [2, 5, 1002, 1005, 2002, 2005, 3002, 3005, -2147483646, -2147483643]:
                    stats['polyline'][vector_id].update({polyid: {}})
                    if vector_id in self.config['vector'].keys():  # if there's a configuration specification
                        stats['polyline'][vector_id][polyid] = {'fields': {}}
                        for _, attr in enumerate(self.config['vector'][vector_id]):  # iterate through each field in config
                            if 'bins' in attr.keys():  # if there's bins
                                bins = attr['bins']
                                tot = 0
                                substats = {bins[i]['label']: 0 for i, bin in enumerate(bins)}
                                for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                                    feature = VectorBase.ogr2shapely(feat, transform)
                                    field_val = feat.GetField(attr['field'])
                                    for _, binn in enumerate(bins):
                                        if 'lower' not in binn.keys():
                                            if field_val < binn['upper']:
                                                substats[binn['label']] += feature.length
                                                tot += feature.length
                                        elif 'upper' not in binn.keys():
                                            if field_val >= binn['lower']:
                                                substats[binn['label']] += feature.length
                                                tot += feature.length
                                        else:
                                            if binn['lower'] <= field_val < binn['upper']:
                                                substats[binn['label']] += feature.length
                                                tot += feature.length

                                stats['polyline'][vector_id][polyid]['fields'].update({attr['field']: substats})
                                stats['polyline'][vector_id][polyid].update({'lengthTotal': tot})

                            else:  # if separated based on categorical values, not bins
                                values_list = []  # get all unique values for field in config
                                tot = 0  # for total length
                                for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                                    # tot.append(fid)
                                    if feat.GetField(attr['field']) not in values_list:
                                        values_list.append(feat.GetField(attr['field']))
                                substats = {str(value): None for value in values_list}
                                for value in values_list:
                                    length = 0
                                    if type(value) == str:
                                        attribute_filter = '{0} = "{1}"'.format(attr['field'], value)
                                    elif type(value) == int:
                                        attribute_filter = '{0} = {1}'.format(attr['field'], value)
                                    for feat, fid, _ in lyr.iterate_features(attribute_filter=attribute_filter, clip_shape=polygon):
                                        feature = VectorBase.ogr2shapely(feat, transform)
                                        length += feature.length
                                        tot += feature.length
                                    substats[str(value)] = length
                                # substats['lengthTotal'] = tot
                                stats['polyline'][vector_id][polyid]['fields'].update({attr['field']: substats})
                                stats['polyline'][vector_id][polyid].update({'lengthTotal': tot})
                    else:
                        length = 0
                        for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                            feature = VectorBase.ogr2shapely(feat, transform)
                            length += feature.length
                        stats['polyline'][vector_id][polyid] = {'lengthTotal': length}

                # POLYGONS
                elif lyr.ogr_geom_type in [3, 6, 1003, 1006, 2003, 2006, 3003, 3006, -2147483645, -2147483642]:
                    stats['polygon'][vector_id].update({polyid: {}})
                    if vector_id in self.config['vector'].keys():  # if there's a coniguration specification
                        stats['polygon'][vector_id][polyid] = {'fields': {}}
                        for _, attr in enumerate(self.config['vector'][vector_id]):  # iterate through each field in config
                            if 'bins' in attr.keys():  # if there's bins
                                bins = attr['bins']
                                tot = 0
                                substats = {bins[i]['label']: 0 for i, bin in enumerate(bins)}
                                for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                                    featureunproj = VectorBase.ogr2shapely(feat)
                                    if not featureunproj.is_valid:
                                        featureunproj = featureunproj.buffer(0)
                                        self.log.warning('feature in polygon {} contains invalid geometry, area may be incorrect'.format(vector_id))
                                    if featureunproj.intersects(polygon):
                                        subfeat = featureunproj.intersection(polygon)
                                        ogrfeat = VectorBase.shapely2ogr(subfeat)
                                        feature = VectorBase.ogr2shapely(ogrfeat, transform)

                                        field_val = feat.GetField(attr['field'])
                                        for _, binn in enumerate(bins):
                                            if 'lower' not in binn.keys():
                                                if field_val < binn['upper']:
                                                    substats[binn['label']] += feature.area
                                                    tot += feature.area
                                                elif 'upper' not in binn.keys():
                                                    if field_val >= binn['lower']:
                                                        substats[binn['label']] += feature.area
                                                        tot += feature.area
                                                else:
                                                    if binn['lower'] <= field_val < binn['upper']:
                                                        substats[binn['label']] += feature.area
                                                        tot += feature.area

                                stats['polygon'][vector_id][polyid]['fields'].update({attr['field']: substats})
                                stats['polygon'][vector_id][polyid].update({'areaTotal': tot})

                            else:  # if separated based on categorical values, not bins
                                values_list = []
                                tot = 0
                                for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                                    if feat.GetField(attr['field']) not in values_list:
                                        values_list.append(feat.GetField(attr['field']))
                                substats = {str(value): None for value in values_list}
                                for value in values_list:
                                    area = 0
                                    if type(value) == str:
                                        attribute_filter = "{0} = '{1}'".format(attr['field'], value)
                                    elif type(value) == int:
                                        attribute_filter = "{0} = {1}".format(attr['field'], value)
                                    for feat, fid, _ in lyr.iterate_features(attribute_filter=attribute_filter, clip_shape=polygon):
                                        featureunproj = VectorBase.ogr2shapely(feat)
                                        if not featureunproj.is_valid:
                                            featureunproj = featureunproj.buffer(0)
                                            self.log.warning('feature in polygon {} contains invalid geometry, area may be incorrect'.format(vector_id))
                                        if featureunproj.intersects(polygon):
                                            subfeat = featureunproj.intersection(polygon)
                                            ogrfeat = VectorBase.shapely2ogr(subfeat)
                                            feature = VectorBase.ogr2shapely(ogrfeat, transform)
                                            area += feature.area
                                            tot += feature.area
                                    substats[str(value)] = area

                                stats['polygon'][vector_id][polyid]['fields'].update({attr['field']: substats})
                                stats['polygon'][vector_id][polyid].update({'areaTotal': tot})

                    else:
                        area = 0
                        for feat, fid, _ in lyr.iterate_features(clip_shape=polygon):
                            featureunproj = VectorBase.ogr2shapely(feat)
                            if not featureunproj.is_valid:
                                featureunproj = featureunproj.buffer(0)
                                self.log.warning('feature in polygon {} contains invalid geometry, area may be incorrect'.format(vector_id))
                            if featureunproj.intersects(polygon):
                                subfeat = featureunproj.intersection(polygon)
                                ogrfeat = VectorBase.shapely2ogr(subfeat)
                                feature = VectorBase.ogr2shapely(ogrfeat, transform)
                                area += feature.area

                        stats['polygon'][vector_id][polyid] = {'areaTotal': area}

        return stats

    def run_metrics(self):
        """Combines the other methods in this class to calculate metrics for raster and vector layers
        within a riverscapes project
        """

        # categorical rasters
        self.log.info('calculating categorical raster metrics')
        if len(self.cat_datasets) > 0:
            for ds_path, ds_ref in self.cat_datasets:
                self.log.info(f'analyzing categorical raster dataset: {ds_ref}')
                if 'gpkg' in ds_path:
                    if not os.path.exists(os.path.dirname(ds_path)):
                        self.log.warning(f'Layer {ds_ref} referenced in project xml but not present in project')
                        continue
                else:
                    if not os.path.exists(ds_path):
                        self.log.warning(f'Layer {ds_ref} referenced in project xml but not present in project')
                        continue
                self.categorical_raster_metrics(ds_ref, ds_path, self.huc8_polygons)
                self.categorical_raster_metrics(ds_ref, ds_path, self.huc12_polygons)

        # floating point rasters
        self.log.info('calculating floating point raster metrics')
        if len(self.float_datasets) > 0:
            for ds_path, ds_ref in self.float_datasets:
                self.log.info(f'analyzing continuous raster dataset: {ds_ref}')
                if 'gpkg' in ds_path:
                    if not os.path.exists(os.path.dirname(ds_path)):
                        self.log.warning(f'Layer {ds_ref} referenced in project xml but not present in project')
                        continue
                else:
                    if not os.path.exists(ds_path):
                        self.log.warning(f'Layer {ds_ref} referenced in project xml but not present in project')
                        continue
                h8stats = raster_buffer_stats2(self.huc8_polygons, ds_path)
                h12stats = raster_buffer_stats2(self.huc12_polygons, ds_path)
                self.rasters_stats_to_metrics(ds_path, h8stats, ds_ref)
                self.rasters_stats_to_metrics(ds_path, h12stats, ds_ref)

                # binned floating points
                if ds_ref in self.config['raster'].keys():
                    bins = self.config['raster'][ds_ref]['bins']
                    h8binned = self.binned_stats(ds_path, ds_ref, self.huc8_polygons, bins)
                    h12binned = self.binned_stats(ds_path, ds_ref, self.huc12_polygons, bins)

                    for key in h12binned[ds_ref].keys():
                        for _, statsdict in enumerate(self.metrics['project']['huc12'][key]['metrics']['raster']['floatingPoint']):
                            if ds_ref in statsdict.keys():
                                statsdict[ds_ref].update({'binnedCounts': h12binned[ds_ref][key]})

                    for key in h8binned[ds_ref].keys():
                        for _, statsdict in enumerate(self.metrics['project']['metrics']['raster']['floatingPoint']):
                            if ds_ref in statsdict.keys():
                                statsdict[ds_ref].update({'binnedCounts': h8binned[ds_ref][key]})

        # vector datasets
        self.log.info('calculating vector metrics')
        if len(self.vector_datasets) > 0:
            for ds_path, ds_ref in self.vector_datasets:
                self.log.info(f'Analyzing vector dataset: {ds_ref}')
                if 'gpkg' in ds_path:
                    if not os.path.exists(os.path.dirname(ds_path)):
                        self.log.warning(f'Layer {ds_ref} referenced in project xml but not present in project')
                        continue
                else:
                    if not os.path.exists(ds_path):
                        self.log.warning(f'Layer {ds_ref} referenced in project xml but not present in project')
                        continue
                h8vectorstats = self.vector_metrics(ds_path, ds_ref, self.huc8_polygons, self.epsg)
                h12vectorstats = self.vector_metrics(ds_path, ds_ref, self.huc12_polygons, self.epsg)

                if len(h8vectorstats['point'][ds_ref].keys()) > 0:
                    for key in h8vectorstats['point'][ds_ref].keys():
                        self.metrics['project']['metrics']['vector']['point'].append({ds_ref: h8vectorstats['point'][ds_ref][key]})

                if len(h8vectorstats['polyline'][ds_ref].keys()) > 0:
                    for key in h8vectorstats['polyline'][ds_ref].keys():
                        self.metrics['project']['metrics']['vector']['polyline'].append({ds_ref: h8vectorstats['polyline'][ds_ref][key]})

                if len(h8vectorstats['polygon'][ds_ref].keys()) > 0:
                    for key in h8vectorstats['polygon'][ds_ref].keys():
                        self.metrics['project']['metrics']['vector']['polygon'].append({ds_ref: h8vectorstats['polygon'][ds_ref][key]})

                if len(h12vectorstats['point'][ds_ref].keys()) > 0:
                    for key in h12vectorstats['point'][ds_ref].keys():
                        self.metrics['project']['huc12'][key]['metrics']['vector']['point'].append({ds_ref: h12vectorstats['point'][ds_ref][key]})

                if len(h12vectorstats['polyline'][ds_ref].keys()) > 0:
                    for key in h12vectorstats['polyline'][ds_ref].keys():
                        self.metrics['project']['huc12'][key]['metrics']['vector']['polyline'].append({ds_ref: h12vectorstats['polyline'][ds_ref][key]})

                if len(h12vectorstats['polygon'][ds_ref].keys()) > 0:
                    for key in h12vectorstats['polygon'][ds_ref].keys():
                        self.metrics['project']['huc12'][key]['metrics']['vector']['polygon'].append({ds_ref: h12vectorstats['polygon'][ds_ref][key]})

    def write_metrics(self):
        """Writes the project metrics dictionary to an output json file
        """
        self.log.info('Writing JSON file')

        with open(os.path.join(os.path.dirname(self.project_xml), 'metrics.json'), 'w') as out_metrics:
            json.dump(self.metrics, out_metrics, indent=4)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('hydro_gpkg', help='Path to the NHDPlus HR geopackage that contains the WBD layers', type=str)
    parser.add_argument('projectxml', help='Path to the project.rs.xml', type=str)
    args = dotenv.parse_args_env(parser)

    instance = MetricsJson(args.hydro_gpkg, args.projectxml)
    instance.run_metrics()
    instance.write_metrics()
