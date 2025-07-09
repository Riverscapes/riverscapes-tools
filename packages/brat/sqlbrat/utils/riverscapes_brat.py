import os
import sqlite3

from rscommons import Logger, ProgressBar, get_shp_or_gpkg, VectorBase
from rscommons.classes.vector_base import get_utm_zone_epsg


def riverscape_brat(gpkg_path: str, windows: dict):
    """
    Args:
        gpkg_path (str): project output geopackage path
        windows (dict): dictionary of moving windows relating dgo IDs to igo IDs
    """

    log = Logger('Riverscapes BRAT')

    reaches = os.path.join(gpkg_path, 'vwReaches')
    dgo = os.path.join(gpkg_path, 'vwDgos')

    log.info('Calculating BRAT Outputs on DGOs')
    with get_shp_or_gpkg(dgo) as dgo_lyr:
        long = dgo_lyr.ogr_layer.GetExtent()[0]
        proj_epsg = get_utm_zone_epsg(long)
        sref, transform = VectorBase.get_transform_from_epsg(dgo_lyr.spatial_ref, proj_epsg)

        dgo_atts = {}
        for dgo_feature, _counter, _progbar in dgo_lyr.iterate_features("Processing DGO features"):
            # st = time.time()
            dgoid = dgo_feature.GetFID()

            dgo_geom = dgo_feature.GetGeometryRef()
            centerline_len = dgo_feature.GetField('centerline_length')
            seg_dist = dgo_feature.GetField('seg_distance')
            if seg_dist is None:
                continue

            ex30 = {}
            ex100 = {}
            hpe30 = {}
            hpe100 = {}
            ex_num_dams = 0
            hist_num_dams = 0
            ex_veg_dams = 0
            hist_veg_dams = 0
            lengths = []
            risk = []
            limitation = []
            opportunity = []
            setting = []
            with get_shp_or_gpkg(reaches) as reach_lyr:
                for reach_feature, _counter, _progbar in reach_lyr.iterate_features(clip_shape=dgo_geom):
                    reach_geom = reach_feature.GetGeometryRef()
                    intersect_geom = reach_geom.Intersection(dgo_geom)
                    if intersect_geom is not None:
                        reach_shapely = VectorBase.ogr2shapely(intersect_geom, transform)
                        reach_length = reach_shapely.length / 1000
                        ex_density = reach_feature.GetField('oCC_EX')
                        hist_density = reach_feature.GetField('oCC_HPE')
                        ex_veg_density = reach_feature.GetField('oVC_EX')
                        hist_veg_density = reach_feature.GetField('oVC_HPE')
                        exveg30 = reach_feature.GetField('iVeg_30EX')
                        exveg100 = reach_feature.GetField('iVeg100EX')
                        hpeveg30 = reach_feature.GetField('iVeg_30HPE')
                        hpeveg100 = reach_feature.GetField('iVeg100HPE')
                        if ex_density is None or hist_density is None:
                            continue
                        ex_num_dams += ex_density * reach_length
                        hist_num_dams += hist_density * reach_length
                        ex_veg_dams += ex_veg_density * reach_length
                        hist_veg_dams += hist_veg_density * reach_length
                        lengths.append(reach_length)
                        risk.append(reach_feature.GetField('Risk'))
                        limitation.append(reach_feature.GetField('Limitation'))
                        opportunity.append(reach_feature.GetField('Opportunity'))
                        setting.append(reach_feature.GetField('Dam_Setting'))
                        ex30[exveg30] = reach_length
                        ex100[exveg100] = reach_length
                        hpe30[hpeveg30] = reach_length
                        hpe100[hpeveg100] = reach_length

            if len(lengths) > 0:
                dgo_atts[dgoid] = {'Lengths': sum(lengths)}
                ix = lengths.index(max(lengths))
                dgo_atts[dgoid]['Risk'] = risk[ix]
                dgo_atts[dgoid]['Limitation'] = limitation[ix]
                dgo_atts[dgoid]['Opportunity'] = opportunity[ix]
                dgo_atts[dgoid]['Dam_Setting'] = setting[ix]
            else:
                dgo_atts[dgoid] = {'Lengths': 0}
                dgo_atts[dgoid]['Risk'] = 'NA'
                dgo_atts[dgoid]['Limitation'] = 'NA'
                dgo_atts[dgoid]['Opportunity'] = 'NA'
                dgo_atts[dgoid]['Dam_Setting'] = 'NA'

            if centerline_len > 0:
                dgo_atts[dgoid]['oCC_EX'] = ex_num_dams / (centerline_len / 1000)
                dgo_atts[dgoid]['oCC_HPE'] = hist_num_dams / (centerline_len / 1000)
                dgo_atts[dgoid]['oVC_EX'] = ex_veg_dams / (centerline_len / 1000)
                dgo_atts[dgoid]['oVC_HPE'] = hist_veg_dams / (centerline_len / 1000)
            else:
                dgo_atts[dgoid]['oCC_EX'] = 0
                dgo_atts[dgoid]['oCC_HPE'] = 0
                dgo_atts[dgoid]['oVC_EX'] = 0
                dgo_atts[dgoid]['oVC_HPE'] = 0

            if len(lengths) > 0:
                dgo_atts[dgoid]['mCC_EX_CT'] = ex_num_dams
                dgo_atts[dgoid]['mCC_HPE_CT'] = hist_num_dams
            else:
                dgo_atts[dgoid]['mCC_EX_CT'] = 0
                dgo_atts[dgoid]['mCC_HPE_CT'] = 0

            if len(ex30) == 0:
                e30, e100, h30, h100 = 0, 0, 0, 0
            elif len(ex30) == 1:
                e30 = list(ex30.keys())[0]
                e100 = list(ex100.keys())[0]
                h30 = list(hpe30.keys())[0]
                h100 = list(hpe100.keys())[0]
            else:
                e30 = sum([k * (v / sum(ex30.values())) for k, v in ex30.items()])
                e100 = sum([k * (v / sum(ex100.values())) for k, v in ex100.items()])
                h30 = sum([k * (v / sum(hpe30.values())) for k, v in hpe30.items()])
                h100 = sum([k * (v / sum(hpe100.values())) for k, v in hpe100.items()])
            dgo_atts[dgoid]['iVeg_30EX'] = e30
            dgo_atts[dgoid]['iVeg100EX'] = e100
            dgo_atts[dgoid]['iVeg_30HPE'] = h30
            dgo_atts[dgoid]['iVeg100HPE'] = h100

    conn = sqlite3.connect(gpkg_path)
    curs = conn.cursor()

    log.info('Updating DGO Attributes')
    progbar = ProgressBar(len(dgo_atts))
    counter = 0
    for dgoid, attrs in dgo_atts.items():
        counter += 1
        progbar.update(counter)
        curs.execute(f"""UPDATE DGOAttributes SET oCC_EX = {attrs['oCC_EX']}, oCC_HPE = {attrs['oCC_HPE']}, oVC_EX = {attrs['oVC_EX']}, oVC_HPE = {attrs['oVC_HPE']},
                        mCC_EX_CT = {attrs['mCC_EX_CT']}, mCC_HPE_CT = {attrs['mCC_HPE_CT']}, iVeg_30EX = {attrs['iVeg_30EX']}, iVeg100EX = {attrs['iVeg100EX']},
                        iVeg_30HPE = {attrs['iVeg_30HPE']}, iVeg100HPE = {attrs['iVeg100HPE']} WHERE DGOID = {dgoid}""")
        curs.execute(f"UPDATE DGOAttributes SET Risk = '{attrs['Risk']}', Limitation = '{attrs['Limitation']}', Opportunity = '{attrs['Opportunity']}', Dam_Setting = '{attrs['Dam_Setting']}' WHERE DGOID = {dgoid}")
        conn.commit()

    log.info('Calculating BRAT Outputs on IGOs (moving window)')
    progbar = ProgressBar(len(windows))
    counter = 0
    for igoid, dgoids in windows.items():
        counter += 1
        progbar.update(counter)
        ex_dams = 0
        hist_dams = 0
        ex_veg_dams = 0
        hist_veg_dams = 0
        cl_len = 0
        area = []
        risk = []
        limitation = []
        opportunity = []
        for dgoid in dgoids:
            curs.execute(f'SELECT centerline_length, oCC_EX, oCC_HPE, oVC_EX, oVC_HPE, Risk, Limitation, Opportunity FROM DGOAttributes WHERE DGOID = {dgoid}')
            dgoattrs = curs.fetchone()
            if dgoattrs[1] is None:
                continue
            cl_len += dgoattrs[0]
            ex_dams += dgoattrs[0]/1000 * dgoattrs[1]
            hist_dams += dgoattrs[0]/1000 * dgoattrs[2]
            ex_veg_dams += dgoattrs[0]/1000 * dgoattrs[3]
            hist_veg_dams += dgoattrs[0]/1000 * dgoattrs[4]
            risk.append(dgoattrs[5])
            limitation.append(dgoattrs[6])
            opportunity.append(dgoattrs[7])
            curs.execute(f'SELECT segment_area FROM DGOAttributes WHERE DGOID = {dgoid}')
            area.append(curs.fetchone()[0])

        if len(area) > 0:
            ix = area.index(max(area))
            risk_val = risk[ix]
            limitation_val = limitation[ix]
            opportunity_val = opportunity[ix]
        else:
            risk_val = None
            limitation_val = None
            opportunity_val = None

        if cl_len > 0:
            curs.execute(f"""UPDATE IGOAttributes SET oCC_EX = {ex_dams / (cl_len / 1000)}, oCC_HPE = {hist_dams / (cl_len / 1000)},
                        oVC_EX = {ex_veg_dams / (cl_len / 1000)}, oVC_HPE = {hist_veg_dams / (cl_len / 1000)} WHERE IGOID = {igoid}""")
            curs.execute(f"UPDATE IGOAttributes SET Risk = '{risk_val}', Limitation = '{limitation_val}', Opportunity = '{opportunity_val}' WHERE IGOID = {igoid}")
            conn.commit()
    conn.close()
    log.info('BRAT DGO and IGO Outputs Calculated')
