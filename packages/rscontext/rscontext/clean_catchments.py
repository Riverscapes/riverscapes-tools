import sqlite3
from osgeo import ogr
from rscommons.classes.vector_base import VectorBase
from rscommons.geometry_ops import shapely_to_ogr_geometry


def clean_nhdplus_catchments(gpkg_path: str, huc_boundary_lyr: str, hucid: str):
    """Removes polygons from the NHDPlusCatchment feature class that are outside of the input watershed boundary.
    """

    conn = sqlite3.connect(gpkg_path)
    curs = conn.cursor()
    curs.execute('CREATE INDEX ux_catchments_nhdplusid ON NHDPlusCatchment(NHDPlusID)')
    curs.execute('CREATE INDEX ux_flowlines_nhdplusid ON NHDFlowline(NHDPlusID)')
    join_data = curs.execute('SELECT NHDPlusCatchment.NHDPlusID, ReachCode FROM NHDPlusCatchment LEFT JOIN NHDFlowline ON NHDPlusCatchment.NHDPlusID = NHDFlowline.NHDPlusID').fetchall()
    join_data_dict = {}
    for i in join_data:
        if i[0] is not None:
            join_data_dict[int(i[0])] = i[1]

    driver = ogr.GetDriverByName('GPKG')
    nhdsrc = driver.Open(gpkg_path)
    huclyr = nhdsrc.GetLayerByName(huc_boundary_lyr)
    hucftrs = [feature for feature in huclyr]
    geom = VectorBase.ogr2shapely(hucftrs[0])

    bound = geom.boundary

    catchlyr = nhdsrc.GetLayerByName("NHDPlusCatchment")
    catchlyr.SetSpatialFilter(shapely_to_ogr_geometry(bound))

    del_ids = []

    for feature in catchlyr:
        if feature.GetField('NHDPlusID') is not None:
            nhdid = int(feature.GetField('NHDPlusID'))
            if join_data_dict[nhdid] is None:
                del_ids.append(nhdid)
            else:
                if hucid[:8] not in join_data_dict[nhdid]:
                    del_ids.append(nhdid)

    for id_str in del_ids:
        curs.execute('DELETE FROM NHDPlusCatchment WHERE NHDPlusCatchment.NHDPlusID = ?', [id_str])
    conn.commit()

    curs.execute('VACUUM')
