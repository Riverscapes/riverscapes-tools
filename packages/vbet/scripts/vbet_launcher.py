
import os
import json
import argparse
from vbet.vbet import vbet
from rscommons.dotenv import parse_dict_env
from rscommons.util import parse_metadata


def main():
    parser = argparse.ArgumentParser(description='VBET Launcher using JSON config')
    parser.add_argument('config', type=str, help='Path to JSON config file')
    parser.add_argument('huc', type=str, help='HUC identifier')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)
    os.environ['HUC'] = args.huc  # Set HUC in environment variables
    parsed_dict = parse_dict_env(config)

    # Allow us to specify a temp folder outside our project folder
    temp_folder = parsed_dict.get('temp_folder', os.path.join(parsed_dict.get('output_dir'), 'temp'))

    meta = parse_metadata(parsed_dict.get('meta', None))
    reach_codes = parsed_dict.get('reach_codes', '').split(',')
    level_paths = parsed_dict.get('level_paths', None)
    level_paths = level_paths if level_paths != ['.'] else None

    vbet(
        parsed_dict.get('flowline_network'),
        parsed_dict.get('dem'),
        parsed_dict.get('slope'),
        parsed_dict.get('hillshade'),
        parsed_dict.get('channel_area'),
        parsed_dict.get('output_dir'),
        parsed_dict.get('huc'),
        flowline_type=parsed_dict.get('flowline_type', 'NHD'),
        unique_stream_field=parsed_dict.get('unique_stream_field', 'level_path'),
        unique_reach_field=parsed_dict.get('unique_reach_field', 'HydroID'),
        drain_area_field=parsed_dict.get('drain_area_field', 'TotDASqKm'),
        in_pitfill_dem=parsed_dict.get('pitfill', None),
        in_dinfflowdir_ang=parsed_dict.get('dinfflowdir_ang', None),
        in_dinfflowdir_slp=parsed_dict.get('dinfflowdir_slp', None),
        debug=parsed_dict.get('debug', False),
        mask=parsed_dict.get('mask', None),
        meta=meta,
        # level_paths=level_paths,
        reach_codes=reach_codes,
        temp_folder=temp_folder
    )


if __name__ == '__main__':
    main()
