"""_summary_
"""

import sqlite3

from rscommons import Logger, ProgressBar


def igo_attributes(db_path: str, windows: dict):
    """_summary_

    Args:
        db_path (str): _description_
        windows (dict): _description_
    """

    log = Logger('BRAT IGO Attributes')

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()

    curs.execute('SELECT DGOID, segment_area FROM DGOAttributes')
    dgoareas = {row[0]: row[1] for row in curs.fetchall()}
    curs.execute('SELECT DGOID, oVC_EX, oVC_HPE, oCC_EX, oCC_HPE FROM DGOAttributes')
    dgoattrs = {row[0]: row[1:] for row in curs.fetchall()}
    curs.execute('SELECT DGOID, Risk, Limitation, Opportunity FROM DGOAttributes')
    categorical = {row[0]: row[1:] for row in curs.fetchall()}

    # veg fis output
    log.info('Performing moving window analyses for IGO BRAT Outputs')
    progbar = ProgressBar(len(windows))
    counter = 0
    for igoid, dgoids in windows.items():
        counter += 1
        progbar.update(counter)
        ovc_ex = []
        ovc_hpe = []
        occ_ex = []
        occ_hpe = []
        area = []
        risk = []
        limitation = []
        opportunity = []
        for dgoid in dgoids:
            if dgoattrs[dgoid][0] is not None:
                ovc_ex.append(dgoattrs[dgoid][0])
                ovc_hpe.append(dgoattrs[dgoid][1])
                occ_ex.append(dgoattrs[dgoid][2])
                occ_hpe.append(dgoattrs[dgoid][3])
                area.append(dgoareas[dgoid])
                risk.append(categorical[dgoid][0])
                limitation.append(categorical[dgoid][1])
                opportunity.append(categorical[dgoid][2])
            else:
                continue

            ovc_ex_val = sum([ovc_ex[i] * (area[i] / sum(area)) for i in range(len(ovc_ex))])
            ovc_hpe_val = sum([ovc_hpe[i] * (area[i] / sum(area)) for i in range(len(ovc_hpe))])
            occ_ex_val = sum([occ_ex[i] * (area[i] / sum(area)) for i in range(len(occ_ex))])
            occ_hpe_val = sum([occ_hpe[i] * (area[i] / sum(area)) for i in range(len(occ_hpe))])
            ix = area.index(max(area))
            risk_val = risk[ix]
            limitation_val = limitation[ix]
            opportunity_val = opportunity[ix]

            curs.execute('UPDATE IGOAttributes SET oVC_EX = ?, oVC_HPE = ?, oCC_EX = ?, oCC_HPE = ? WHERE IGOID = ?', (ovc_ex_val, ovc_hpe_val, occ_ex_val, occ_hpe_val, igoid))
            curs.execute('UPDATE IGOAttributes SET Risk = ?, Limitation = ?, Opportunity = ? WHERE IGOID = ?', (risk_val, limitation_val, opportunity_val, igoid))
            conn.commit()
