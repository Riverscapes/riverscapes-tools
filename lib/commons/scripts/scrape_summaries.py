"""I think the purpose of this script is to produce a table of metrics that BLM
folks could use for additional metrics beyond what is going into the paragraph"""

import sqlite3
import csv

db = '/workspaces/data/test_data/rme_scrape_output_1601.sqlite'
csv_out = '/workspaces/data/test_data/scrape_table.csv'

conn = sqlite3.connect(db)
curs = conn.cursor()

# metrics
metrics = {'Riverscape Length (mi)': ['SELECT sum(dgo_length_miles) FROM vw_metrics WHERE ownership IS NULL;',
                                      'SELECT sum(dgo_length_miles) FROM vw_metrics WHERE us_state = (?) AND ownership IS NULL;',
                                      'SELECT sum(dgo_length_miles) FROM vw_metrics WHERE us_state = (?) AND fcode IN (?) and ownership IS NULL;',
                                      'SELECT sum(dgo_length_miles) FROM vw_metrics WHERE us_state = (?) AND fcode IN (?) AND ownership = (?);'],
           'Riverscape Area (acres)': ['SELECT sum(dgo_area_acres) FROM vw_metrics WHERE ownership IS NULL;',
                                       'SELECT sum(dgo_area_acres) FROM vw_metrics WHERE us_state = (?) and ownership IS NULL;',
                                       'SELECT sum(dgo_area_acres) FROM vw_metrics WHERE us_state = (?) AND fcode IN (?) AND ownership IS NULL;',
                                       'SELECT sum(dgo_area_acres) FROM vw_metrics WHERE us_state = (?) AND fcode IN (?) AND ownership = (?);'],
           'Proportion Active': ["""SELECT sum((active / area) * (area / tot_area)) FROM (SELECT active_area active,
                                    dgo_area_acres area, (SELECT sum(dgo_area_acres) FROM vw_metrics WHERE ownership IS NULL) tot_area
                                    FROM vw_metrics WHERE ownership IS NULL);""",
                                 """SELECT sum((active / area) * (area / tot_area)) FROM (SELECT active_area active,
                                dgo_area_acres area, (SELECT sum(dgo_area_acres) FROM vw_metrics where us_state = (?) AND ownership IS NULL) tot_area
                                FROM vw_metrics WHERE us_state = (?) AND ownership IS NULL);""",
                                 """SELECT sum((active / area) * (area / tot_area)) FROM (SELECT active_area active,
                                dgo_area_acres area, (SELECT sum(dgo_area_acres) FROM vw_metrics where us_state = (?) and fcode IN (?) AND ownership IS NULL) tot_area
                                FROM vw_metrics WHERE us_state = (?) AND fcode IN (?) AND ownership IS NULL);""",
                                 """SELECT sum((active / area) * (area / tot_area)) FROM (SELECT active_area active,
                                dgo_area_acres area, (SELECT sum(dgo_area_acres) FROM vw_metrics where us_state = (?) and fcode IN (?) and ownership = (?)) tot_area
                                FROM vw_metrics WHERE us_state = (?) AND fcode IN (?) AND ownership = (?));"""],
           'Recovery Potential': ['']}

# get states
curs.execute('SELECT DISTINCT us_state FROM vw_metrics')
states_pres = [row[0] for row in curs.fetchall()]
curs.execute('SELECT name, where_clause FROM us_states')
states = {row[0]: row[1] for row in curs.fetchall() if row[1] in states_pres}

# get ownerships
curs.execute('SELECT DISTINCT ownership FROM vw_metrics')
owners_pres = [row[0] for row in curs.fetchall()]
curs.execute('SELECT name, where_clause FROM owners')
owners = {row[0]: row[1] for row in curs.fetchall() if row[1] in owners_pres}

# get flow types
curs.execute('SELECT name, where_clause FROM flows')
flows = {row[0]: row[1] for row in curs.fetchall()}

with open(csv_out, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['Metric', 'State', 'Flow', 'Ownership', 'Value'])

    for metric, query in metrics.items():
        curs.execute(query[0])
        total = curs.fetchone()[0]
        writer.writerow([metric, '-', '-', '-', total])

        for state_lab, state in states.items():
            # length of riverscape
            if metric in ['Riverscape Length (mi)', 'Riverscape Area (acres)']:
                curs.execute(query[1], (state,))
            else:
                curs.execute(query[1], (state, state))
            miles = curs.fetchone()[0]
            writer.writerow([metric, state, '-', '-', miles])

            for flow_lab, flow in flows.items():
                if metric in ['Riverscape Length (mi)', 'Riverscape Area (acres)']:
                    curs.execute(query[2], (state, flow_lab))
                else:
                    curs.execute(query[2], (state, flow_lab, state, flow_lab))
                miles = curs.fetchone()[0]
                writer.writerow([metric, state, flow_lab, '-', miles])

                for owner_lab, owner in owners.items():
                    if metric in ['Riverscape Length (mi)', 'Riverscape Area (acres)']:
                        curs.execute(query[3], (state, flow_lab, owner_lab))
                    else:
                        curs.execute(query[3], (state, flow_lab, owner_lab, state, flow_lab, owner_lab))
                    miles = curs.fetchone()[0]
                    writer.writerow([metric, state, flow_lab, owner_lab, miles])
