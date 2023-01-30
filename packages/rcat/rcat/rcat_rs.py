"""
Augment RCAT with the power of riverscapes context
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject, RSMeta, dotenv, Logger
from rcat.rcat_report import RcatReport

lyrs_in_out = {
    # RCAT_ID: INPUT_ID
    'EXVEG': ['EXVEG'],
    'HISTVEG': ['HISTVEG'],
    'flowareas': ['NHDArea'],
    'waterbodies': ['NHDWaterbody'],
    'PITFILL': ['PITFILL'],
    'igo': ['IGOGeometry'],
    'reaches': ['ReachGeometry'],
    'roads': ['roads'],
    'rails': ['rails'],
    'canals': ['canals'],
    'valley_bottom': ['vbet_full'],
    'dgo': ['vbet_dgos']
}


def main():

    parser = argparse.ArgumentParser(
        description='Anthro XML Augmenter',
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
        gpkg_path = os.path.join(out_prj.project_dir, out_prj.XMLBuilder.find('.//Outputs/Geopackage[@id="OUTPUTS"]/Path').text)

        in_xmls = args.in_xmls.split(',')
        rscontext_xml = in_xmls[0]
        taudem_xml = in_xmls[1]
        anthro_xml = in_xmls[2]
        vbet_xml = in_xmls[3]
        out_prj.rs_copy_project_extents(rscontext_xml)
        rscproj = RSProject(None, rscontext_xml)
        taudemproj = RSProject(None, taudem_xml)
        anthroproj = RSProject(None, anthro_xml)
        vbetproj = RSProject(None, vbet_xml)

        # get watershed
        watershed_node = rscproj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            proj_watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
            if proj_watershed_node is None:
                out_prj.add_metadata([RSMeta('Watershed', watershed_node.text)])

        # add rsx paths to output xml
        done = []  # list of found nodes so that they don't get repeated if they exist in two projects
        for outid, inid in lyrs_in_out.items():
            for l in rscproj.XMLBuilder.tree.iter():
                if 'lyrName' in l.attrib.keys():
                    if l.attrib['lyrName'] == inid[0]:
                        if inid[0] not in done:
                            innode = l
                            proj = rscproj
                            done.append(inid[0])
                if 'id' in l.attrib.keys():
                    if l.attrib['id'] == inid[0]:
                        if inid[0] not in done:
                            innode = l
                            proj = rscproj
                            done.append(inid[0])
            for m in taudemproj.XMLBuilder.tree.iter():
                if 'lyrName' in m.attrib.keys():
                    if m.attrib['lyrName'] == inid[0]:
                        if inid[0] not in done:
                            innode = m
                            proj = taudemproj
                            done.append(inid[0])
                if 'id' in m.attrib.keys():
                    if m.attrib['id'] == inid[0]:
                        if inid[0] not in done:
                            innode = m
                            proj = taudemproj
                            done.append(inid[0])
            for n in anthroproj.XMLBuilder.tree.iter():
                if 'lyrName' in n.attrib.keys():
                    if n.attrib['lyrName'] == inid[0]:
                        if inid[0] not in done:
                            innode = n
                            proj = anthroproj
                            done.append(inid[0])
                if 'id' in n.attrib.keys():
                    if n.attrib['id'] == inid[0]:
                        if inid[0] not in done:
                            innode = n
                            proj = anthroproj
                            done.append(inid[0])
            for o in vbetproj.XMLBuilder.tree.iter():
                if 'lyrName' in o.attrib.keys():
                    if o.attrib['lyrName'] == inid[0]:
                        if inid[0] not in done:
                            innode = o
                            proj = vbetproj
                            done.append(inid[0])
                if 'id' in o.attrib.keys():
                    if o.attrib['id'] == inid[0]:
                        if inid[0] not in done:
                            innode = o
                            proj = vbetproj
                            done.append(inid[0])
            if not innode:
                raise Exception(f'dataset with id {inid[0]} not found in any input project xmls')

            path = proj.get_rsx_path(innode)
            lyrs_in_out[outid].append(path)
            lyrs_in_out[outid].append(proj.XMLBuilder.find('Warehouse').attrib['id'])

            for p in out_prj.XMLBuilder.tree.iter():
                if 'lyrName' in p.attrib.keys():
                    if p.attrib['lyrName'] == outid:
                        p.attrib['extRef'] = lyrs_in_out[outid][2] + ':' + lyrs_in_out[outid][1]
                if 'id' in p.attrib.keys():
                    if p.attrib['id'] == outid:
                        p.attrib['extRef'] = lyrs_in_out[outid][2] + ':' + lyrs_in_out[outid][1]

        # if watershed in meta, change the project name
        watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            name_node = out_prj.XMLBuilder.find('Name')
            name_node.text = f"BRAT for {watershed_node.text}"

        out_prj.XMLBuilder.write()
        report_path = out_prj.XMLBuilder.find('.//HTMLFile[@id="REPORT"]/Path').text
        report = RcatReport(os.path.join(out_prj.project_dir, report_path), out_prj)
        report.write()

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
