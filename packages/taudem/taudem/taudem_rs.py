"""
Augment TauDEM with the power of riverscapes context
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject, RSMeta, dotenv, Logger
from taudem.taudem_report import TauDEMReport

lyrs_in_out = {
    # ID: INPUT_ID
    'HILLSHADE': ['HILLSHADE'],
    'DEM': ['DEM'],
    'channel_area': ['channel_area'],
    'GDAL_SLOPE': ['SLOPE']
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
        # out_prj.rs_meta_augment(
        #     args.in_xmls.split(','),
        #     lyrs_in_out
        # )

        in_xmls = args.in_xmls.split(',')
        rscontext_xml = in_xmls[0]
        chan_xml = in_xmls[1]
        out_prj.rs_copy_project_extents(rscontext_xml)
        rscproj = RSProject(None, rscontext_xml)
        chanproj = RSProject(None, chan_xml)

        # get watershed
        watershed_node = rscproj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            proj_watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
            if proj_watershed_node is None:
                out_prj.add_metadata([RSMeta('Watershed', watershed_node.text)])

        # add rsx paths to output xml
        for outid, inid in lyrs_in_out.items():
            for n in rscproj.XMLBuilder.tree.iter():
                if 'lyrName' in n.attrib.keys():
                    if n.attrib['lyrName'] == inid[0]:
                        innode = n
                        proj = rscproj
                if 'id' in n.attrib.keys():
                    if n.attrib['id'] == inid[0]:
                        innode = n
                        proj = rscproj
            for m in chanproj.XMLBuilder.tree.iter():
                if 'lyrName' in m.attrib.keys():
                    if m.attrib['lyrName'] == inid[0]:
                        innode = m
                        proj = chanproj
                if 'id' in m.attrib.keys():
                    if m.attrib['id'] == inid[0]:
                        innode = m
                        proj = chanproj
            if not innode:
                raise Exception(f'dataset with id {inid[0]} not found in any input project xmls')

            path = proj.get_rsx_path(innode)
            lyrs_in_out[outid].append(path)
            lyrs_in_out[outid].append(proj.XMLBuilder.find('Warehouse').attrib['id'])

            for o in out_prj.XMLBuilder.tree.iter():
                if 'lyrName' in o.attrib.keys():
                    if o.attrib['lyrName'] == outid:
                        o.attrib['extRef'] = lyrs_in_out[outid][2] + ':' + lyrs_in_out[outid][1]
                if 'id' in o.attrib.keys():
                    if o.attrib['id'] == outid:
                        o.attrib['extRef'] = lyrs_in_out[outid][2] + ':' + lyrs_in_out[outid][1]

        # if watershed in meta, change the project name
        watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            name_node = out_prj.XMLBuilder.find('Name')
            name_node.text = f"TauDEM for {watershed_node.text}"

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
