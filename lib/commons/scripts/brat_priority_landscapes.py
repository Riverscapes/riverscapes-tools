import os
import json

huc10_metrics_json = ''
landscape_metrics_json = ''
priority_landscape_json = ''

brat_metrics = {
    'bratCapacity': {
            'perennial': {
                'miles': {
                    'pervasive': None,
                    'frequent': None,
                    'occasional': None,
                    'rare': None,
                    'none': None
                    }
                },
            'intermittent': {
                'miles': {
                    'pervasive': None,
                    'frequent': None,
                    'occasional': None,
                    'rare': None,
                    'none': None
                    }
                }
            },
        'bratRisk': {
            'BLM': {
                'perennial': {
                    'miles': None
                },
                'intermittent': {
                    'miles': None
                }
            },
            'All': {
                'perennial': {
                    'miles': None
                },
                'intermittent': {
                    'miles': None
                }
            }
        },
        'bratLimitation': {
            'BLM': {
                'perennial': {
                    'miles': None
                },
                'intermittent': {
                    'miles': None
                }
            },
            'All': {
                'perennial': {
                    'miles': None
                },
                'intermittent': {
                    'miles': None
                }
            }
        },
        'bratOpportunity': {
            'BLM': {
                'perennial': {
                    'miles': None
                },
                'intermittent': {
                    'miles': None
                }
            },
            'All': {
                'perennial': {
                    'miles': None
                },
                'intermittent': {
                    'miles': None
                }
            }
        }
}

with open(huc10_metrics_json, 'r') as f:
    huc10_metrics = json.load(f)

with open(landscape_metrics_json, 'r') as f:
    landscape_metrics = json.load(f)

with open(priority_landscape_json, 'r') as f:
    priority_landscape = json.load(f)

huc_lookup = {}
for key, val in priority_landscape.items():
    for huc in val:
        huc_lookup[huc] = key

for huc, metrics in huc10_metrics.items():
    if huc_lookup[huc[:8]] in brat_metrics.keys():
        for key, val in huc10_metrics[huc]['bratCapacity']['perennial']['miles']['pervasive'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['pervasive'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['pervasive'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['pervasive'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['perennial']['miles']['frequent'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['frequent'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['frequent'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['frequent'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['perennial']['miles']['occasional'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['occasional'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['occasional'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['occasional'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['perennial']['miles']['rare'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['rare'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['rare'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['rare'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['perennial']['miles']['none'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['none'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['none'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['none'][key] = val

        for key, val in huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['pervasive'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['pervasive'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['pervasive'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['pervasive'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['frequent'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['frequent'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['frequent'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['frequent'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['occasional'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['occasional'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['occasional'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['occasional'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['rare'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['rare'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['rare'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['rare'][key] = val
        for key, val in huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['none'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['none'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['none'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['none'][key] = val

        for key, val in huc10_metrics[huc]['bratRisk']['BLM']['perennial']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['perennial']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['perennial']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['perennial']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratRisk']['BLM']['intermittent']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['intermittent']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['intermittent']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['intermittent']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratRisk']['All']['perennial']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['perennial']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['perennial']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['perennial']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratRisk']['All']['intermittent']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['intermittent']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['intermittent']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['intermittent']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratLimitation']['BLM']['perennial']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['perennial']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['perennial']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['perennial']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratLimitation']['BLM']['intermittent']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['intermittent']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['intermittent']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['intermittent']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratLimitation']['All']['perennial']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['perennial']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['perennial']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['perennial']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratLimitation']['All']['intermittent']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['intermittent']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['intermittent']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['intermittent']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratOpportunity']['BLM']['perennial']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['perennial']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['perennial']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['perennial']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratOpportunity']['BLM']['intermittent']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['intermittent']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['intermittent']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['intermittent']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratOpportunity']['All']['perennial']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['perennial']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['perennial']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['perennial']['miles'][key] = val

        for key, val in huc10_metrics[huc]['bratOpportunity']['All']['intermittent']['miles'].items():
            if key in brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['intermittent']['miles'].keys():
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['intermittent']['miles'][key] += val
            else:
                brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['intermittent']['miles'][key] = val

    else:
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['pervasive'] = huc10_metrics[huc]['bratCapacity']['perennial']['miles']['pervasive']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['frequent'] = huc10_metrics[huc]['bratCapacity']['perennial']['miles']['frequent']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['occasional'] = huc10_metrics[huc]['bratCapacity']['perennial']['miles']['occasional']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['rare'] = huc10_metrics[huc]['bratCapacity']['perennial']['miles']['rare']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['perennial']['miles']['none'] = huc10_metrics[huc]['bratCapacity']['perennial']['miles']['none']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['pervasive'] = huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['pervasive']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['frequent'] = huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['frequent']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['occasional'] = huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['occasional']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['rare'] = huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['rare']
        brat_metrics[huc_lookup[huc[:8]]]['bratCapacity']['intermittent']['miles']['none'] = huc10_metrics[huc]['bratCapacity']['intermittent']['miles']['none']
        brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['perennial']['miles'] = huc10_metrics[huc]['bratRisk']['BLM']['perennial']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['BLM']['intermittent']['miles'] = huc10_metrics[huc]['bratRisk']['BLM']['intermittent']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['perennial']['miles'] = huc10_metrics[huc]['bratRisk']['All']['perennial']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratRisk']['All']['intermittent']['miles'] = huc10_metrics[huc]['bratRisk']['All']['intermittent']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['perennial']['miles'] = huc10_metrics[huc]['bratLimitation']['BLM']['perennial']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['BLM']['intermittent']['miles'] = huc10_metrics[huc]['bratLimitation']['BLM']['intermittent']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['perennial']['miles'] = huc10_metrics[huc]['bratLimitation']['All']['perennial']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratLimitation']['All']['intermittent']['miles'] = huc10_metrics[huc]['bratLimitation']['All']['intermittent']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['perennial']['miles'] = huc10_metrics[huc]['bratOpportunity']['BLM']['perennial']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['BLM']['intermittent']['miles'] = huc10_metrics[huc]['bratOpportunity']['BLM']['intermittent']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['perennial']['miles'] = huc10_metrics[huc]['bratOpportunity']['All']['perennial']['miles']
        brat_metrics[huc_lookup[huc[:8]]]['bratOpportunity']['All']['intermittent']['miles'] = huc10_metrics[huc]['bratOpportunity']['All']['intermittent']['miles']

for key in landscape_metrics.keys():
    if key in brat_metrics:
        for bkey, bval in brat_metrics[key].items():
            landscape_metrics[key][bkey] = bval
