import sqlite3

def get_metrics(gpkg_path):

    out_metrics = {
        'bratCapacity': {
            'perennial': {
                'miles': {
                    'pervasive': None,
                    'frequent': None,
                    'occasional': None,
                    'rare': None,
                    'none': None
                    },
                'km': {
                    'pervasive': None,
                    'frequent': None,
                    'occasional': None,
                    'rare': None,
                    'none': None
                    },
                'avCapacity': {
                    'BLM': None,
                    'All': None
                    }
                },
            'intermittent': {
                'miles': {
                    'pervasive': None,
                    'frequent': None,
                    'occasional': None,
                    'rare': None,
                    'none': None
                    },
                'km': {
                    'pervasive': None,
                    'frequent': None,
                    'occasional': None,
                    'rare': None,
                    'none': None
                    },
                'avCapacity': {
                    'BLM': None,
                    'All': None
                    }
                }
            },
        'bratRisk': {
            'BLM': {
                'perennial': {
                    'miles': None,
                    'km': None
                },
                'intermittent': {
                    'miles': None,
                    'km': None
                }
            },
            'All': {
                'perennial': {
                    'miles': None,
                    'km': None
                },
                'intermittent': {
                    'miles': None,
                    'km': None
                }
            }
        },
        'bratLimitation': {
            'BLM': {
                'perennial': {
                    'miles': None,
                    'km': None
                },
                'intermittent': {
                    'miles': None,
                    'km': None
                }
            },
            'All': {
                'perennial': {
                    'miles': None,
                    'km': None
                },
                'intermittent': {
                    'miles': None,
                    'km': None
                }
            }
        },
        'bratOpportunity': {
            'BLM': {
                'perennial': {
                    'miles': None,
                    'km': None
                },
                'intermittent': {
                    'miles': None,
                    'km': None
                }
            },
            'All': {
                'perennial': {
                    'miles': None,
                    'km': None
                },
                'intermittent': {
                    'miles': None,
                    'km': None
                }
            }
        }
    }
    
    conn = sqlite3.connect(gpkg_path)
    curs = conn.cursor()

    # Capacity metrics
    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE oCC_EX > 15 AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_perv = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['miles']['pervasive'] = peren_perv

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE oCC_EX > 15 AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_perv_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['km']['pervasive'] = peren_perv_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE (oCC_EX <= 15 AND oCC_EX > 5) AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_freq = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['miles']['frequent'] = peren_freq

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE (oCC_EX <= 15 AND oCC_EX > 5) AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_freq_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['km']['frequent'] = peren_freq_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE (oCC_EX <= 5 AND oCC_EX > 1) AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_occ = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['miles']['occasional'] = peren_occ

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE (oCC_EX <= 5 AND oCC_EX > 1) AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_occ_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['km']['occasional'] = peren_occ_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE (oCC_EX <= 1 AND oCC_EX > 0) AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_rare = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['miles']['rare'] = peren_rare

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE (oCC_EX <= 1 AND oCC_EX > 0) AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_rare_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['km']['rare'] = peren_rare_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE oCC_EX = 0 AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_none = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['miles']['none'] = peren_none

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE oCC_EX = 0 AND ReachType = 'Perennial' GROUP BY Agency""")
    peren_none_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['perennial']['km']['none'] = peren_none_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE oCC_EX > 15 AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_perv = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['miles']['pervasive'] = inter_perv

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE oCC_EX > 15 AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_perv_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['km']['pervasive'] = inter_perv_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE (oCC_EX <= 15 AND oCC_EX > 5) AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_freq = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['miles']['frequent'] = inter_freq

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE (oCC_EX <= 15 AND oCC_EX > 5) AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_freq_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['km']['frequent'] = inter_freq_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE (oCC_EX <= 5 AND oCC_EX > 1) AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_occ = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['miles']['occasional'] = inter_occ

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE (oCC_EX <= 5 AND oCC_EX > 1) AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_occ_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['km']['occasional'] = inter_occ_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE (oCC_EX <= 1 AND oCC_EX > 0) AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_rare = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['miles']['rare'] = inter_rare

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE (oCC_EX <= 1 AND oCC_EX > 0) AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_rare_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['km']['rare'] = inter_rare_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Agency FROM vwReaches WHERE oCC_EX = 0 AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_none = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['miles']['none'] = inter_none

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Agency FROM vwReaches WHERE oCC_EX = 0 AND ReachType = 'Intermittent' GROUP BY Agency""")
    inter_none_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratCapacity']['intermittent']['km']['none'] = inter_none_km

    # Average capacity
    curs.execute("""SELECT AVG(oCC_EX) FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path')""")
    av_cap_blm_peren = curs.fetchone()[0]
    out_metrics['bratCapacity']['perennial']['avCapacity']['BLM'] = av_cap_blm_peren

    curs.execute("""SELECT AVG(oCC_EX) FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND ReachType = 'Intermittent'""")
    av_cap_blm_int = curs.fetchone()[0]
    out_metrics['bratCapacity']['intermittent']['avCapacity']['BLM'] = av_cap_blm_int

    curs.execute("""SELECT AVG(oCC_EX) FROM vwReaches WHERE (ReachType = 'Perennial' OR ReachType = 'Artificial Path')""")
    av_cap_all_peren = curs.fetchone()[0]
    out_metrics['bratCapacity']['perennial']['avCapacity']['All'] = av_cap_all_peren

    curs.execute("""SELECT AVG(oCC_EX) FROM vwReaches WHERE ReachType = 'Intermittent'""")
    av_cap_all_int = curs.fetchone()[0]
    out_metrics['bratCapacity']['intermittent']['avCapacity']['All'] = av_cap_all_int

    # Risk
    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Risk FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Risk""")
    risk_blm_peren = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['BLM']['perennial']['miles'] = risk_blm_peren

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Risk FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Risk""")
    risk_blm_peren_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['BLM']['perennial']['km'] = risk_blm_peren_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Risk FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND ReachType = 'Intermittent' GROUP BY Risk""")
    risk_blm_int = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['BLM']['intermittent']['miles'] = risk_blm_int

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Risk FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND ReachType = 'Intermittent' GROUP BY Risk""")
    risk_blm_int_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['BLM']['intermittent']['km'] = risk_blm_int_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Risk FROM vwReaches WHERE (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Risk""")
    risk_all_peren = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['All']['perennial']['miles'] = risk_all_peren

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Risk FROM vwReaches WHERE (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Risk""")
    risk_all_peren_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['All']['perennial']['km'] = risk_all_peren_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Risk FROM vwReaches WHERE ReachType = 'Intermittent' GROUP BY Risk""")
    risk_all_int = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['All']['intermittent']['miles'] = risk_all_int

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Risk FROM vwReaches WHERE ReachType = 'Intermittent' GROUP BY Risk""")
    risk_all_int_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratRisk']['All']['intermittent']['km'] = risk_all_int_km

    # Limitation
    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Limitation FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Limitation""")
    lim_blm_peren = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['BLM']['perennial']['miles'] = lim_blm_peren

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Limitation FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Limitation""")
    lim_blm_peren_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['BLM']['perennial']['km'] = lim_blm_peren_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Limitation FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND ReachType = 'Intermittent' GROUP BY Limitation""")
    lim_blm_int = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['BLM']['intermittent']['miles'] = lim_blm_int

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Limitation FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND ReachType = 'Intermittent' GROUP BY Limitation""")
    lim_blm_int_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['BLM']['intermittent']['km'] = lim_blm_int_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Limitation FROM vwReaches WHERE (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Limitation""")
    lim_all_peren = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['All']['perennial']['miles'] = lim_all_peren

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Limitation FROM vwReaches WHERE (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Limitation""")
    lim_all_peren_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['All']['perennial']['km'] = lim_all_peren_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Limitation FROM vwReaches WHERE ReachType = 'Intermittent' GROUP BY Limitation""")
    lim_all_int = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['All']['intermittent']['miles'] = lim_all_int

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Limitation FROM vwReaches WHERE ReachType = 'Intermittent' GROUP BY Limitation""")
    lim_all_int_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratLimitation']['All']['intermittent']['km'] = lim_all_int_km

    # Opportunity
    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Opportunity FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Opportunity""")
    opp_blm_peren = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['BLM']['perennial']['miles'] = opp_blm_peren

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Opportunity FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Opportunity""")
    opp_blm_peren_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['BLM']['perennial']['km'] = opp_blm_peren_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Opportunity FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND ReachType = 'Intermittent' GROUP BY Opportunity""")
    opp_blm_int = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['BLM']['intermittent']['miles'] = opp_blm_int

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Opportunity FROM vwReaches WHERE Agency = 'Bureau of Land Management' AND ReachType = 'Intermittent' GROUP BY Opportunity""")
    opp_blm_int_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['BLM']['intermittent']['km'] = opp_blm_int_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Opportunity FROM vwReaches WHERE (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Opportunity""")
    opp_all_peren = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['All']['perennial']['miles'] = opp_all_peren

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Opportunity FROM vwReaches WHERE (ReachType = 'Perennial' OR ReachType = 'Artificial Path') GROUP BY Opportunity""")
    opp_all_peren_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['All']['perennial']['km'] = opp_all_peren_km

    curs.execute("""SELECT SUM(iGeo_Len) / 1000 * 0.621371 miles, Opportunity FROM vwReaches WHERE ReachType = 'Intermittent' GROUP BY Opportunity""")
    opp_all_int = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['All']['intermittent']['miles'] = opp_all_int

    curs.execute("""SELECT SUM(iGeo_Len) / 1000, Opportunity FROM vwReaches WHERE ReachType = 'Intermittent' GROUP BY Opportunity""")
    opp_all_int_km = {row[1]: row[0] for row in curs.fetchall()}
    out_metrics['bratOpportunity']['All']['intermittent']['km'] = opp_all_int_km

    return out_metrics
