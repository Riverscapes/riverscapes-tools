""" dds the conservation and restoration model to the BRAT capacity output

    Philip Bailey, adapted from pyBRAT3 script by Sara Bangen
    25 Oct 2019
"""
import os
import sys
import traceback
import argparse
from rscommons import Logger, dotenv
from rscommons.database import load_attributes, write_db_attributes, SQLiteCon


def conservation(database: str, max_da: float = None):
    """Calculate conservation fields for an existing BRAT database
    Assumes that conflict attributes and the dam capacity model
    have already been run for the database

    Arguments:
        database {str} -- Path to BRAT database
    """

    results = calculate_conservation(database, max_da)
    write_db_attributes(database, results, ['OpportunityID', 'LimitationID', 'RiskID'])


def calculate_conservation(database: str, max_da: float = None):
    """ Perform conservation calculations

    Args:
        database (str): path to BRAT geopackage

    Returns:
        dict: dictionary of conservation values keyed by Reach ID
    """

    log = Logger('Conservation')

    # Verify all the input fields are present and load their values
    reaches = load_attributes(database,
                              ['ReachCode', 'oVC_HPE', 'oVC_EX', 'oCC_HPE', 'oCC_EX', 'iGeo_Slope', 'iPC_VLowLU', 'iPC_HighLU', 'iPC_LU', 'iPC_RoadX', 'iPC_RoadVB', 'iPC_RailVB', 'iHyd_SPLow', 'iHyd_SP2', 'iPC_Canal', 'Dam_Setting', 'iGeo_DA'],
                              '(oCC_EX IS NOT NULL)')

    log.info('Calculating conservation for {:,} reaches.'.format(len(reaches)))

    risks = load_lookup(database, 'SELECT Name, RiskID AS ID FROM DamRisks')
    limitations = load_lookup(database, 'SELECT Name, LimitationID AS ID FROM DamLimitations')
    opportunties = load_lookup(database, 'SELECT Name, OpportunityID AS ID FROM DamOpportunities')

    for values in reaches.values():

        # Areas beavers can build dams, but could have undesireable impacts
        values['RiskID'] = calc_risks(risks, values['oCC_EX'], values['iPC_LU'], values['iPC_Canal'], values['iPC_RoadX'], values['iPC_RoadVB'], values['iPC_RailVB'])

        # Areas beavers can't build dams and why
        values['LimitationID'] = calc_limited(limitations, values['oVC_HPE'], values['oVC_EX'], values['oCC_EX'], values['iGeo_Slope'], values['iPC_LU'], values['iHyd_SPLow'], values['iHyd_SP2'], values['iGeo_DA'], max_da)

        # Conservation and restoration opportunties
        values['OpportunityID'] = calc_opportunities(opportunties, risks, values['RiskID'], values['ReachCode'], values['oCC_EX'], values['oVC_EX'], values['iPC_VLowLU'], values['iPC_HighLU'], values['Dam_Setting'])

    log.info('Conservation calculation complete')
    return reaches


def calc_risks(risks: dict, occ_ex: float, ipc_lu: float, ipc_canal: float, ipc_roadx: float, ipc_roadvb: float, ipc_railvb: float) -> int:
    """ Calculate risk values

    Args:
        risks (dict): risk type lookup
        occ_ex ([type]): [description]
        opc_dist ([type]): [description]
        ipc_lu ([type]): [description]
        ipc_canal ([type]): [description]

    Raises:
        Exception: [description]

    Returns:
        [type]: [description]
    """

    if occ_ex <= 0:
        # if capacity is none risk is negligible
        return risks['Negligible Risk']
    elif ipc_canal is not None and ipc_canal <= 20:
        # if canals are within 20 meters (usually means canal is on the reach)
        return risks['Considerable Risk']
    else:
        conf_inputs = [input for input in [ipc_roadx, ipc_roadvb, ipc_railvb] if input is not None]
        if len(conf_inputs) > 0:
            if min(conf_inputs) is not None and ipc_lu is not None:
                # if infrastructure within 30 m or land use is high
                # if capacity is frequent or pervasive risk is considerable
                # if capaicty is rare or ocassional risk is some
                if min(conf_inputs) <= 30 or ipc_lu >= 66:
                    if occ_ex >= 5.0:
                        return risks['Considerable Risk']
                    else:
                        return risks['Some Risk']
                # if infrastructure within 30 to 100 m
                # if capacity is frequent or pervasive risk is some
                # if capaicty is rare or ocassional risk is minor
                elif min(conf_inputs) <= 100:
                    if occ_ex >= 5.0:
                        return risks['Some Risk']
                    else:
                        return risks['Minor Risk']
                # if infrastructure within 100 to 300 m or land use is 0.33 to 0.66 risk is minor
                elif min(conf_inputs) <= 300 or ipc_lu >= 33:
                    return risks['Minor Risk']
                else:
                    return risks['Negligible Risk']
            else:
                return risks['Negligible Risk']
        else:
            return risks['Negligible Risk']

    raise Exception('Unhandled undesireable dam risk')


def calc_limited(limitations: dict, ovc_hpe: float, ovc_ex: float, occ_ex: float, slope: float, landuse: float, splow: float, sp2: float, da: float, max_da: float) -> int:
    """ Calculate limitations to conservation

    Args:
        limitations ([type]): [description]
        ovc_hpe ([type]): [description]
        ovc_ex ([type]): [description]
        occ_ex ([type]): [description]
        slope ([type]): [description]
        landuse ([type]): [description]
        splow ([type]): [description]
        sp2 ([type]): [description]

    Raises:
        Exception: [description]

    Returns:
        [type]: [description]
    """

    # First deal with vegetation limitations
    # Find places historically veg limited first ('oVC_HPE' None)
    if ovc_hpe is not None and ovc_hpe <= 0:
        # 'oVC_EX' Occasional, Frequent, or Pervasive (some areas have oVC_EX > oVC_HPE)
        if ovc_ex is not None and ovc_ex > 0:
            return limitations['Potential Reservoir or Land Use Change']
        else:
            return limitations['Naturally Vegetation Limited']
    # 'iGeo_Slope' > 23%
    elif slope is not None and slope > 0.23:
        return limitations['Slope Limited']
    elif (splow is not None and splow >= 190) or (sp2 is not None and sp2 >= 2400):
        return limitations['Stream Power Limited']
    # 'oCC_EX' None (Primary focus of this layer is the places that can't support dams now... so why?)
    elif occ_ex is not None and occ_ex <= 0:
        if max_da is not None and da is not None and da > max_da:
            return limitations['Stream Size Limited']
        elif (max_da is not None and da is not None and da < max_da) or max_da is None:
            if landuse is not None and landuse > 30:
                return limitations['Anthropogenically Limited']
            else:
                return limitations['Other']
        else:
            return limitations['Other']
    else:
        return limitations['Dam Building Possible']

    raise Exception('Unhandled dam limitation')


def calc_opportunities(opportunities: dict, risks: dict, risk_id: float, reachcode: int, occ_ex: float, ovc_ex: float, ipc_vlowlu: float, ipc_highlu: float, dam_setting: str) -> int:
    """ Calculate conservation opportunities

    Args:
        opportunities ([type]): [description]
        risks ([type]): [description]
        risk_id ([type]): [description]
        occ_hpe ([type]): [description]
        occ_ex ([type]): [description]
        mcc_hisdep ([type]): [description]
        ipc_vlowlu ([type]): [description]
        ipc_highlu ([type]): [description]

    Raises:
        Exception: [description]

    Returns:
        [type]: [description]
    """

    if occ_ex is not None and reachcode is not None and ipc_vlowlu is not None and ipc_highlu is not None:
        if reachcode in (46006, 55800):
            if dam_setting in ('Classic', 'Steep'):
                if risk_id == risks['Negligible Risk'] or risk_id == risks['Minor Risk']:
                    if occ_ex > 10:
                        if ipc_vlowlu > 90:
                            return opportunities['Conservation/Appropriate for Translocation']
                        else:
                            return opportunities['Encourage Beaver Expansion/Colonization']
                    elif occ_ex >= 5 and occ_ex < 10:
                        return opportunities['Encourage Beaver Expansion/Colonization']
                    else:
                        if ipc_highlu >= 25 or ovc_ex >= 5:
                            return opportunities['Natural or Anthropogenic Limitations']
                        else:
                            return opportunities['Address Resource Limitations']
                else:
                    if occ_ex > 10:
                        if ipc_highlu >= 25:
                            return opportunities['Appropriate for Beaver Mimicry']
                        else:
                            return opportunities['Encourage Beaver Expansion/Colonization']
                    elif occ_ex >= 5 and occ_ex < 10:
                        if ipc_highlu >= 25:
                            return opportunities['Natural or Anthropogenic Limitations']
                        else:
                            return opportunities['Appropriate for Beaver Mimicry']
                    else:
                        if ipc_highlu >= 25 or ovc_ex >= 5:
                            return opportunities['Natural or Anthropogenic Limitations']
                        else:
                            return opportunities['Address Resource Limitations']

            elif dam_setting == 'Floodplain':
                if risk_id == risks['Negligible Risk'] or risk_id == risks['Minor Risk']:
                    if ovc_ex >= 5:
                        if ipc_highlu > 25:
                            return opportunities['Natural or Anthropogenic Limitations']
                        else:
                            return opportunities['Potential Floodplain/Side Channel Opportunities']
                    else:
                        return opportunities['Natural or Anthropogenic Limitations']
                else:
                    return opportunities['Natural or Anthropogenic Limitations']
            else:
                return opportunities['Natural or Anthropogenic Limitations']

        elif reachcode == 46003:
            if dam_setting in ('Classic', 'Steep'):
                if ipc_highlu >= 25:
                    return opportunities['Natural or Anthropogenic Limitations']
                else:
                    return opportunities['Appropriate for Beaver Mimicry']
            else:
                return opportunities['Natural or Anthropogenic Limitations']
        else:
            return opportunities['Natural or Anthropogenic Limitations']
    else:
        return opportunities['Natural or Anthropogenic Limitations']

    raise Exception('Unhandled conservation opportunity')


def load_lookup(gpkg_path: str, sql: str) -> dict:
    """ Load one of the conservation tables as a dictionary

    Args:
        gpkg_path (str): [description]
        sql (str): [description]

    Returns:
        dict: [description]
    """

    with SQLiteCon(gpkg_path) as database:
        database.curs.execute(sql)
        return {row['Name']: row['ID'] for row in database.curs.fetchall()}


def main():
    """ Conservation
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='BRAT SQLite database', type=str)
    parser.add_argument('--verbose', help='(optional) verbose logging mode', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger('Conservation')
    logfile = os.path.join(os.path.dirname(args.database), 'conservation.log')
    log.setup(logPath=logfile, verbose=args.verbose)

    try:
        conservation(args.database)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
