import os
import argparse
import json
import sys
import traceback
import uuid
import datetime
from rscommons import Logger, RSProject, RSLayer, ModelConfig, dotenv, initGDALOGRErrors
from rscommons.util import safe_makedirs, safe_remove_file

cfg = ModelConfig('http://xml.riverscapes.xyz/Projects/XSD/V1/LST.xsd', '0.0.1')


def process_lst(lst_xml_folder):
    """This is a slightly hack-y script to create some XMLS for the land_surface_temp script
        It's a bit of an afterthought so it just plunks down the XMLS all alone in a folder

    Args:
        lst_xml_folder ([type]): [description]
    """

    log = Logger("Generate XMLS for LST")
    hucs = [str(1700 + x) for x in range(1, 13)]

    for huc in hucs:
        hucdir = os.path.join(lst_xml_folder, huc)
        xml_file = os.path.join(hucdir, 'project.rs.xml')
        safe_makedirs(hucdir)
        if os.path.exists(xml_file):
            safe_remove_file(xml_file)

        project_name = f'Land Surface Temperature for HUC {huc}'
        project = RSProject(cfg, xml_file)
        project.create(project_name, 'LST')

        project.add_metadata({
            'ModelVersion': cfg.version,
            'HUC': huc,
            'dateCreated': datetime.datetime.now().isoformat(),
            'HUC{}'.format(len(huc)): huc
        })

        realizations = project.XMLBuilder.add_sub_element(project.XMLBuilder.root, 'Realizations')
        realization = project.XMLBuilder.add_sub_element(realizations, 'LST', None, {
            'id': 'LST1',
            'dateCreated': datetime.datetime.now().isoformat(),
            'guid': str(uuid.uuid4()),
            'productVersion': cfg.version
        })
        project.XMLBuilder.add_sub_element(realization, 'Name', project_name)

        output_node = project.XMLBuilder.add_sub_element(realization, 'Outputs')
        zipfile_node = project.add_dataset(output_node, f'{huc}.zip', RSLayer(f'LST Result for {huc}', 'LST_ZIP', 'ZipFile', '1706.zip'), 'ZipFile', replace=True, rel_path=True)

        project.XMLBuilder.write()
    log.info('done')


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('lst_xml_folder', help='Top level data folder containing LST data', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser, os.path.join(os.path.dirname(__file__), '.env'))

    # Initiate the log file
    log = Logger('Land Surface Temperature XML Generator')
    log.setup(logPath=os.path.join(os.path.dirname(args.lst_xml_folder), 'lst_xml.log'), verbose=args.verbose)

    try:
        process_lst(args.lst_xml_folder)
        log.info('Process completed successfully')
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
