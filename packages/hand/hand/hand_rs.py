"""
Augment HAND with the power of riverscapes context
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject
from rsxml import dotenv, Logger
from hand.hand_report import HANDReport

lyrs_in_out = {
    # ID: INPUT_ID
    'HILLSHADE': 'HILLSHADE',
    'FLOWLINES': 'NHDFlowline',
    'FLOW_AREA': 'NHDArea'
}


def main():

    parser = argparse.ArgumentParser(
        description='HAND XML Augmenter',
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

        out_prj.XMLBuilder.write()
        report_path = out_prj.XMLBuilder.find('.//HTMLFile[@id="REPORT"]/Path').text
        report = HANDReport(os.path.join(out_prj.project_dir, report_path), out_prj)
        report.write()

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
