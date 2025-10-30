import os
import json
from rsxml.util import safe_remove_dir
from cybercastor.lib.file_download import download_files

from brat_proj_metrics import get_metrics

# download_files('production', 'vbet', 1601020202, 'vbet.gpkg')
priority_hucs = '/mnt/c/Users/jordang/Documents/LTPBR/BLM_Priority_Watersheds/BLMPriorityHUC8_sub.json'

in_json = '/mnt/c/Users/jordang/Documents/Riverscapes/data/huc10_metrics.json'

brat_dir = '/mnt/c/Users/jordang/Documents/Riverscapes/data/brat'

huc_list = []

with open(priority_hucs, 'r', encoding='utf-8') as f:
    hucs = json.load(f)
for name, hucs in hucs.items():
    for huc in hucs:
        huc_list.append(huc)

with open(in_json, 'r', encoding='utf-8') as f:
    huc10_metrics = json.load(f)

for huc in huc_list:
    try:
        download_files('production', 'brat', huc, 'brat.gpkg')
    except Exception as e:
        print(f'failed to download huc in {huc}')
        print(e)
        continue
    for direc in os.listdir(brat_dir):
        if str(huc) in direc:
            brat_gpkg = os.path.join(brat_dir, direc, 'brat.gpkg')
            brat_metrics = get_metrics(brat_gpkg)
            if direc not in huc10_metrics.keys():
                huc10_metrics[direc] = {
                    'bratCapacity': None,
                    'bratRisk': None,
                    'bratLimitation': None,
                    'bratOpportunity': None
                }
            huc10_metrics[direc]['bratCapacity'] = brat_metrics['bratCapacity']
            huc10_metrics[direc]['bratRisk'] = brat_metrics['bratRisk']
            huc10_metrics[direc]['bratLimitation'] = brat_metrics['bratLimitation']
            huc10_metrics[direc]['bratOpportunity'] = brat_metrics['bratOpportunity']

            safe_remove_dir(os.path.join(brat_dir, direc))

with open(in_json, 'w', encoding='utf-8') as f:
    json.dump(huc10_metrics, f, indent=4)

print('done summarizing huc10s')
