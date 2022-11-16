"""
Augment Channel with the power of riverscapes context
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject, RSMeta, dotenv, Logger
from channel.channel_report import ChannelReport

lyrs_in_out = {
    'flowlines': ['NHDFlowline'],
    'flowareas': ['NHDArea'],
    'waterbody': ['NHDWaterbody'],
}


def main():

    parser = argparse.ArgumentParser(
        description='Channel XML Augmenter',
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

        in_xml = args.in_xmls.split(',')[0]
        inprj = RSProject(None, in_xml)

        warehouse_guid = inprj.XMLBuilder.find('Warehouse').attrib['id']
        watershed_node = inprj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            proj_watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
            if proj_watershed_node is None:
                out_prj.add_metadata([RSMeta('Watershed', watershed_node.text)])

        for outid, inid in lyrs_in_out.items():
            # find the node and get is ref
            for n in inprj.XMLBuilder.tree.iter():
                if 'lyrName' in n.attrib.keys():
                    if n.attrib['lyrName'] == inid[0]:
                        innode = n
                elif 'id' in n.attrib.keys():
                    if n.attrib['id'] == inid[0]:
                        innode = n
                else:
                    continue
            path = inprj.get_rsx_path(innode)
            lyrs_in_out[outid].append(path)

            # add the rsxpath to the output xml
            for m in out_prj.XMLBuilder.tree.iter():
                if 'lyrName' in m.attrib.keys():
                    if m.attrib['lyrName'] == outid:
                        m.attrib['extRef'] = warehouse_guid + ':' + lyrs_in_out[outid][1]
                elif 'id' in m.attrib.keys():
                    if m.attrib['id'] == outid:
                        m.attrib['extRef'] = warehouse_guid + ':' + lyrs_in_out[outid][1]

        out_prj.rs_copy_project_extents(in_xml)

        # if watershed in meta, change the project name
        watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            name_node = out_prj.XMLBuilder.find('Name')
            name_node.text = f"Channel Area for {watershed_node.text}"

        out_prj.XMLBuilder.write()
        report_path = out_prj.XMLBuilder.find('.//HTMLFile[@id="REPORT"]/Path').text
        report = ChannelReport(os.path.join(out_prj.project_dir, report_path), out_prj)
        report.write()

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
