"""
Augment Riverscapes Metrics with the power of riverscapes context
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject, RSMeta, dotenv, Logger
from rme.utils.rme_report import RMEReport

lyrs_in_out = {
    'flowlines': 'NHDFlowline',
    'ownership': 'Ownership',
    'states': 'States',
    'counties': 'Counties',
    'DEM': 'DEM',
    'Precip': 'Precip',
    'roads': 'Roads',
    'rail': 'Rail',
    'vbet_dgos': 'vbet_dgos',
    'vbet_igos': 'vbet_igos',
    'vbet_centerlines': 'vbet_centerlines',
    'confinement_dgos': 'vwDgos',
    'anthro_dgos': 'vwDgos',
    'rcat_dogs': 'vwDgos'
}


def main():
    """augment riverscapes metrics engine project xml
    """
    parser = argparse.ArgumentParser(
        description='Riverscapes Metric Engine XML Augmenter',
        # epilog="This is an epilog"
    )
    parser.add_argument('out_project_xml', help='Input XML file', type=str)
    parser.add_argument('in_xmls', help='Comma-separated list of XMLs in decreasing priority', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger('XML Augmenter')
    log.setup(verbose=args.verbose)
    log.title(f'XML Augmenter: {args.out_project_xml}')

    try:
        out_prj = RSProject(None, args.out_project_xml)
        out_prj.rs_meta_augment(
            args.in_xmls.split(','),
            lyrs_in_out
        )

        in_xmls = args.in_xmls.split(',')
        rscontext_xml = in_xmls[0]
        out_prj.rs_copy_project_extents(rscontext_xml)
        rscproj = RSProject(None, rscontext_xml)

        # get watershed
        watershed_node = rscproj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            proj_watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
            if proj_watershed_node is None:
                out_prj.add_metadata([RSMeta('Watershed', watershed_node.text)])

        # if watershed in meta, change the project name
        watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            name_node = out_prj.XMLBuilder.find('Name')
            name_node.text = f"RCAT for {watershed_node.text}"

        out_prj.XMLBuilder.write()
        report_path = out_prj.XMLBuilder.find('.//HTMLFile[@id="REPORT"]/Path').text
        geopackage_path = out_prj.XMLBuilder.find('.//Geopackage[@id="RME_OUTPUTS"]/Path').text
        report = RMEReport(os.path.join(out_prj.project_dir, geopackage_path), os.path.join(out_prj.project_dir, report_path), out_prj)
        report.write()

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
