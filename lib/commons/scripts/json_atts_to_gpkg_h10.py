from rscommons import GeopackageLayer
import json

in_json = '/mnt/c/Users/jordang/Documents/Riverscapes/data/huc10_metrics.json'
in_gpkg = '/mnt/c/Users/jordang/Documents/Riverscapes/data/landscape_synthesis/Blackfoot-ClarkFork/Blackfoot_ClarkFork.gpkg/Blackfoot_ClarkFork'

with open(in_json, 'r') as f:
    metrics = json.load(f)

with GeopackageLayer(in_gpkg, write=True) as lyr:
    for ftr, *_ in lyr.iterate_features():
        name = ftr.GetField('HUC10')
        print(name)
        if name is not None:
            ftr.SetField('miles_perennial', sum(metrics[name]['channelLengths']['perennial']['miles'].values()))
            if 'BLM' in metrics[name]['channelLengths']['perennial']['miles'].keys():
                ftr.SetField('blm_miles_perennial', metrics[name]['channelLengths']['perennial']['miles']['BLM'])
            else:
                ftr.SetField('blm_miles_perennial', 0)
            ftr.SetField('miles_intermittent', sum(metrics[name]['channelLengths']['intermittent']['miles'].values()))
            if 'BLM' in metrics[name]['channelLengths']['intermittent']['miles'].keys():
                ftr.SetField('blm_miles_intermittent', metrics[name]['channelLengths']['intermittent']['miles']['BLM'])
            else:
                ftr.SetField('blm_miles_intermittent', 0)
            if len(metrics[name]['channelLengths']['ephemeral']['miles'].values()) > 0:
                ftr.SetField('miles_ephemeral', sum(metrics[name]['channelLengths']['ephemeral']['miles'].values()))
            else:
                ftr.SetField('miles_ephemeral', 0)
            if 'BLM' in metrics[name]['channelLengths']['ephemeral']['miles'].keys():
                ftr.SetField('blm_miles_ephemeral', metrics[name]['channelLengths']['ephemeral']['miles']['BLM'])
            else:
                ftr.SetField('blm_miles_ephemeral', 0)
            ftr.SetField('acres_perennial_vb', sum(metrics[name]['valleyBottomAreas']['perennial']['acres'].values()))
            if 'BLM' in metrics[name]['valleyBottomAreas']['perennial']['acres'].keys():
                ftr.SetField('blm_acres_perennial_vb', metrics[name]['valleyBottomAreas']['perennial']['acres']['BLM'])
            else:
                ftr.SetField('blm_acres_perennial_vb', 0)
            ftr.SetField('intermittent_acres_vb', sum(metrics[name]['valleyBottomAreas']['intermittent']['acres'].values()))
            if 'BLM' in metrics[name]['valleyBottomAreas']['intermittent']['acres'].keys():
                ftr.SetField('blm_intermittent_acres_vb', metrics[name]['valleyBottomAreas']['intermittent']['acres']['BLM'])
            else:
                ftr.SetField('blm_intermittent_acres_vb', 0)
            if len(metrics[name]['valleyBottomAreas']['ephemeral']['acres'].values()) > 0:
                ftr.SetField('eph_acres_vb', sum(metrics[name]['valleyBottomAreas']['ephemeral']['acres'].values()))
            else:
                ftr.SetField('eph_acres_vb', 0)
            if 'BLM' in metrics[name]['valleyBottomAreas']['ephemeral']['acres'].keys():
                ftr.SetField('blm_eph_acres_vb', metrics[name]['valleyBottomAreas']['ephemeral']['acres']['BLM'])
            else:
                ftr.SetField('blm_eph_acres_vb', 0)
            ftr.SetField('perennial_acres_riparian', sum(metrics[name]['riparianCover']['perennial']['acres'].values()))
            if 'BLM' in metrics[name]['riparianCover']['perennial']['acres'].keys():
                ftr.SetField('blm_perennial_acres_riparian', metrics[name]['riparianCover']['perennial']['acres']['BLM'])
            else:
                ftr.SetField('blm_perennial_acres_riparian', 0)
            ftr.SetField('intermittent_acres_riparian', sum(metrics[name]['riparianCover']['intermittent']['acres'].values()))
            if 'BLM' in metrics[name]['riparianCover']['intermittent']['acres'].keys():
                ftr.SetField('blm_intermittent_acres_riparian', metrics[name]['riparianCover']['intermittent']['acres']['BLM'])
            else:
                ftr.SetField('blm_intermittent_acres_riparian', 0)
            if len(metrics[name]['riparianCover']['ephemeral']['acres'].values()) > 0:
                ftr.SetField('eph_acres_riparian', sum(metrics[name]['riparianCover']['ephemeral']['acres'].values()))
            else:
                ftr.SetField('eph_acres_riparian', 0)
            if 'BLM' in metrics[name]['riparianCover']['ephemeral']['acres'].keys():
                ftr.SetField('blm_eph_acres_riparian', metrics[name]['riparianCover']['ephemeral']['acres']['BLM'])
            else:
                ftr.SetField('blm_eph_acres_riparian', 0)

            lyr.ogr_layer.SetFeature(ftr)

print('done')
