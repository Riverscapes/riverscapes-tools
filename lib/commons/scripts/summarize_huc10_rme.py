import os
import json
from rscommons.util import safe_remove_dir
from cybercastor.lib.file_download import download_files

from rme_proj_metrics import get_rme_metrics

priority_hucs = '/mnt/c/Users/jordang/Documents/LTPBR/BLM_Priority_Watersheds/BLMPriorityHUC8_sub.json'

in_json = '/mnt/c/Users/jordang/Documents/Riverscapes/data/huc10_metrics.json'

rme_dir = '/mnt/c/Users/jordang/Documents/Riverscapes/data/rme'

huc_list = []

with open(priority_hucs, 'r') as f:
    hucs = json.load(f)
for name, hucs in hucs.items():
    for huc in hucs:
        huc_list.append(huc)

with open(in_json, 'r') as f:
    huc10_metrics = json.load(f)

for huc in huc_list:
    try:
        download_files('production', 'rs_metric_engine', huc, 'riverscapes_metrics.gpkg')
    except Exception as e:
        print(f'failed to download huc in {huc}')
        print(e)
        continue
    for direc in os.listdir(rme_dir):
        if str(huc) in direc:
            rme_gpkg = os.path.join(rme_dir, direc, 'riverscapes_metrics.gpkg')
            rme_metrics = get_rme_metrics(rme_gpkg)
            if direc not in huc10_metrics.keys():
                huc10_metrics[direc] = {
                    'channelArea': None,
                    'lowlyingArea': None,
                    'elevatedArea': None,
                    'valleyWidth': None
                }
            huc10_metrics[direc]['channelArea'] = rme_metrics['channelArea']
            huc10_metrics[direc]['lowlyingArea'] = rme_metrics['lowlyingArea']
            huc10_metrics[direc]['elevatedArea'] = rme_metrics['elevatedArea']
            huc10_metrics[direc]['valleyWidth'] = rme_metrics['valleyWidth']

            safe_remove_dir(os.path.join(rme_dir, direc))

with open(in_json, 'w') as f:
    json.dump(huc10_metrics, f, indent=4)

print('done summarizing huc10s')