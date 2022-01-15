"""
Augment TauDEM with the power of riverscapes context
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject, dotenv, Logger
from taudem.taudem_report import TauDEMReport

lyrs_in_out = {
    # ID: INPUT_ID
    'HILLSHADE': 'HILLSHADE',
    'DEM': 'DEM',
    'CHANNEL_AREA': 'CHANNEL_AREA'
}


def main():

    parser = argparse.ArgumentParser(
        description='TauDEM XML Augmenter',
        # epilog="This is an epilog"
    )
    parser.add_argument('out_project_xml', help='Input XML file', type=str)
    parser.add_argument('in_xmls', help='Comma-separated list of XMLs in decreasing priority', type=str)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Initiate the log file
    log = Logger('XML Augmenter')
    log.setup(verbose=args.verbose)
    log.title('XML Augmenter: {}'.format(args.out_project_xml))

    try:
        out_prj = RSProject(None, args.out_project_xml)
        out_prj.rs_meta_augment(
            args.in_xmls.split(','),
            lyrs_in_out
        )

        in_xml = args.in_xmls.split(',')[0]
        out_prj.rs_copy_project_extents(in_xml)

        out_prj.XMLBuilder.write()
        report_path = out_prj.XMLBuilder.find('.//HTMLFile[@id="REPORT"]/Path').text
        report = TauDEMReport(os.path.join(out_prj.project_dir, report_path), out_prj)
        report.write()

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
