"""
Augment BLM Context XML with RS Meta data from input projects
"""
import argparse
import traceback
import sys
import os
from rscommons import RSProject
from rsxml import dotenv, Logger
from blm_context.blm_context_report import BLMContextReport

lyrs_in_out = {
    # BLM_CONTEXT_ID: INPUT_ID
    'NHDFlowline': 'NHDFlowline',
    'vw_NHDFlowlineVAA': 'vw_NHDFlowlineVAA',
    'vw_NHDPlusCatchmentVAA': 'vw_NHDPlusCatchmentVAA',
    'NHDArea': 'NHDArea',
    'NHDPlusCatchment': 'NHDPlusCatchment',
    'NHDWaterbody': 'NHDWaterbody',
    'NHDPlusFlowlineVAA': 'NHDPlusFlowlineVAA',
    'WBDHU2': 'WBDHU2',
    'WBDHU4': 'WBDHU4',
    'WBDHU6': 'WBDHU6',
    'WBDHU8': 'WBDHU8',
    'WBDHU10': 'WBDHU10',
    'WBDHU12': 'WBDHU12',
    'buffered_clip100m': 'buffered_clip100m',
    'buffered_clip500m': 'buffered_clip500m',
    'network_crossings': 'network_crossings',
    'network_intersected': 'network_intersected',
    'network_segmented': 'network_segmented',
    'catchments': 'catchments',
    'processing_extent': 'processing_extent',
    'NHDAreaSplit': 'NHDAreaSplit',
    'NHDWaterbodySplit': 'NHDWaterbodySplit',
    'vbet_full': 'vbet_full',
    'low_lying_valley_bottom': 'low_lying_valley_bottom',
    'low_lying_floodplain': 'low_lying_floodplain',
    'elevated_floodplain': 'elevated_floodplain',
    'floodplain': 'floodplain',
    'vbet_centerlines': 'vbet_centerlines',
    'vbet_igos': 'vbet_igos',
    'HILLSHADE': 'HILLSHADE',
    'USFWS_Critical_Habitat_A': 'USFWS_Critical_Habitat_A',
    'USFWS_Critical_Habitat_L': 'USFWS_Critical_Habitat_L',
    'NIFC_Fuel_Polys': 'NIFC_Fuel_Polys',
    'BLM_Natl_Fire_Perimeters_P': 'BLM_Natl_Fire_Perimeters_P',
    'BLM_Natl_Visual_Resource_Inventory_Classes_Polygon_A': 'BLM_Natl_Visual_Resource_Inventory_Classes_Polygon_A',
    'BLM_Natl_Area_Critical_Env_Concern_A': 'BLM_Natl_Area_Critical_Env_Concern_A',
    'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_A': 'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_A',
    'BLM_Natl_Grazing_Allotment_P': 'BLM_Natl_Grazing_Allotment_P',
    'BLM_Natl_WesternUS_GRSG_ROD_Habitat_Mgmt_Areas_Aug22_A"': 'BLM_Natl_WesternUS_GRSG_ROD_Habitat_Mgmt_Areas_Aug22_A"',
    'BLM_Natl_Land_Use_Plans_2022_A': 'BLM_Natl_Land_Use_Plans_2022_A',
    'BLM_Natl_Revision_Development_Land_Use_Plans_A': 'BLM_Natl_Revision_Development_Land_Use_Plans_A',
    'BLM_ES_SO_Natl_Scenic_Historic_Trails_NLCS_L': 'BLM_ES_SO_Natl_Scenic_Historic_Trails_NLCS_L',
    'BLM_Natl_Recreation_Site_Polygons': 'BLM_Natl_Recreation_Site_Polygons',
    'BLM_Restoration_Landscapes_A': 'BLM_Restoration_Landscapes_A',
    'DOI_Keystone_Initiatives_A': 'DOI_Keystone_Initiatives_A',
    'BLM_Natl_NLCS_Wilderness_Areas_A': 'BLM_Natl_NLCS_Wilderness_Areas_A',
    'BLM_Natl_NLCS_Wilderness_Study_Areas_A': 'BLM_Natl_NLCS_Wilderness_Study_Areas_A',
    'BLM_NLCS_Natl_Monuments_Cons_Areas_A': 'BLM_NLCS_Natl_Monuments_Cons_Areas_A'
}


def main():
    """Augment BLM Context XML with RS Meta data from input projects
    """

    parser = argparse.ArgumentParser(
        description='BLM Context XML Augmenter',
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

        in_xml = args.in_xmls.split(',')[0]
        out_prj.rs_copy_project_extents(in_xml)

        # if watershed in meta, change the project name
        watershed_node = out_prj.XMLBuilder.find('MetaData').find('Meta[@name="Watershed"]')
        if watershed_node is not None:
            name_node = out_prj.XMLBuilder.find('Name')
            name_node.text = f"BLM Context for {watershed_node.text}"

        out_prj.XMLBuilder.write()
        # report_path = out_prj.XMLBuilder.find('.//HTMLFile[@id="REPORT"]/Path').text
        # report = BLMContextReport(os.path.join(out_prj.project_dir, report_path), out_prj)
        # report.write()

    except Exception as err:
        log.error(err)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
