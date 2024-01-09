from rscommons import GeopackageLayer
import json

in_json = '/mnt/c/Users/jordang/Documents/LTPBR/BLM_Priority_Watersheds/priority_landscape_metrics_11723.json'
in_gpkg = '/mnt/c/Users/jordang/Documents/Riverscapes/data/landscape_synthesis/BLM_priority_landscapes/BLM_restoration_landscapes.gpkg/BLM_restoration_landscapes'

with open(in_json, 'r') as f:
    metrics = json.load(f)

with GeopackageLayer(in_gpkg, write=True) as lyr:
    for ftr, *_ in lyr.iterate_features():
        name = ftr.GetField('RestorationLandscape_Name')
        print(name)
        if name is not None:
            ftr.SetField('miles_perennial', metrics[name]['All']['perennial']['milesChannel'])
            ftr.SetField('blm_miles_perennial', metrics[name]['BLM']['perennial']['milesChannel'])
            ftr.SetField('miles_intermittent', metrics[name]['All']['intermittent']['milesChannel'])
            ftr.SetField('blm_miles_intermittent', metrics[name]['BLM']['intermittent']['milesChannel'])
            ftr.SetField('miles_ephemeral', metrics[name]['All']['ephemeral']['milesChannel'])
            ftr.SetField('blm_miles_ephemeral', metrics[name]['BLM']['ephemeral']['milesChannel'])
            ftr.SetField('acres_perennial_vb', metrics[name]['All']['perennial']['acresVB'])
            ftr.SetField('blm_acres_perennial_vb', metrics[name]['BLM']['perennial']['acresVB'])
            ftr.SetField('intermittent_acres_vb', metrics[name]['All']['intermittent']['acresVB'])
            ftr.SetField('blm_intermittent_acres_vb', metrics[name]['BLM']['intermittent']['acresVB'])
            ftr.SetField('eph_acres_vb', metrics[name]['All']['ephemeral']['acresVB'])
            ftr.SetField('blm_eph_acres_vb', metrics[name]['BLM']['ephemeral']['acresVB'])
            ftr.SetField('perennial_acres_riparian', metrics[name]['All']['perennial']['acresRiparian'])
            ftr.SetField('blm_perennial_acres_riparian', metrics[name]['BLM']['perennial']['acresRiparian'])
            ftr.SetField('intermittent_acres_riparian', metrics[name]['All']['intermittent']['acresRiparian'])
            ftr.SetField('blm_intermittent_acres_riparian', metrics[name]['BLM']['intermittent']['acresRiparian'])
            ftr.SetField('eph_acres_riparian', metrics[name]['All']['ephemeral']['acresRiparian'])
            ftr.SetField('blm_eph_acres_riparian', metrics[name]['BLM']['ephemeral']['acresRiparian'])

            lyr.ogr_layer.SetFeature(ftr)

print('done')
