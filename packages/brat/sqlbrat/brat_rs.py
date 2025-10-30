"""
Augment BRAT with the power of riverscapes context
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject, RSMeta
from rsxml import dotenv, Logger
from sqlbrat.brat_report import BratReport

lyrs_in_out = {
    # BRAT_ID: INPUT_ID
    'HILLSHADE': ['HILLSHADE', 'RSContext'],
    'EXVEG': ['EXVEG', 'RSContext'],
    'HISTVEG': ['HISTVEG', 'RSContext'],
    'flowareas': ['NHDArea', 'RSContext'],
    'waterbodies': ['NHDWaterbody', 'RSContext'],
    'hydro_flowlines': ['vwReaches', 'hydro_context'],
    'hydro_igos': ['vwIgos', 'hydro_context'],
    'hydro_dgos': ['vwDgos', 'hydro_context'],
    'anthro_flowlines': ['vwReaches', 'Anthro'],
    'anthro_igos': ['vwIgos', 'Anthro'],
    'anthro_dgos': ['vwDgos', 'Anthro'],
    'valley_bottom': ['vbet_full', 'VBET']
}


def main():

    parser = argparse.ArgumentParser(
        description='BRAT XML Augmenter',
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
        gpkg_path = os.path.join(out_prj.project_dir, out_prj.XMLBuilder.find('.//Outputs/Geopackage[@id="OUTPUTS"]/Path').text)

        in_xmls = args.in_xmls.split(',')
        rscontext_xml = in_xmls[0]
        vbet_xml = in_xmls[1]
        out_prj.rs_copy_project_extents(rscontext_xml)
        rscproj = RSProject(None, rscontext_xml)
        vbetproj = RSProject(None, vbet_xml)

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
            name_node.text = f"BRAT for {watershed_node.text}"

        out_prj.XMLBuilder.write()
        report_path = out_prj.XMLBuilder.find('.//HTMLFile[@id="BRAT_RUN_REPORT"]/Path').text
        report = BratReport(gpkg_path, os.path.join(out_prj.project_dir, report_path), out_prj)
        report.write()

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
