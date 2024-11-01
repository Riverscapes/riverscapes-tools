import sqlite3

db = '/data/riverscapes_production.gpkg'


def get_huc8s():
    
    with sqlite3.connect(db) as conn:
        curs = conn.cursor()
        curs.execute('''
            SELECT DISTINCT huc
            FROM sites
            WHERE huc IS NOT NULL
            ORDER BY huc
        ''')
        hucs = curs.fetchall()

    # yield the huc8
    for huc in hucs:
        yield huc[0]


def calculate_regional_curve():

    count = 0
    processed = 0

    with sqlite3.connect(db) as conn:
        curs = conn.cursor()
        curs.execute('''
            CREATE TABLE IF NOT EXISTS regional_curves
            (
                huc8 TEXT UNIQUE NOT NULL,
                median_peak_discharge REAL,
                median_min_discharge REAL
            )
        ''')


    for huc in get_huc8s():
        print(huc)
        count += 1

        with sqlite3.connect(db) as conn:
            curs = conn.cursor()
            curs.execute('''
                SELECT s.site_no, d.peak_discharge, d.min_discharge
                FROM sites s
                JOIN discharges d ON s.site_no = d.site_no
                WHERE s.huc = ? AND d.is_valid = 1
            ''', (huc,))
            sites_with_discharges = curs.fetchall()

            print(f'   sites: {len(sites_with_discharges)}')
            # Get discharges
            huc_discharges = []
            for site in sites_with_discharges:
                # site[0] is site_no, site[1] is peak_discharge, site[2] is min_discharge
                huc_discharges.append({
                    'site_no': site[0],
                    'peak_discharge': site[1],
                    'min_discharge': site[2]
                })

            # Sort huc_discharges by peak discharge
            huc_discharges.sort(key=lambda x: x['peak_discharge'])

            if len(huc_discharges) > 10:
                # Sort and pick 10 across the huc8
                pick_count = 10 - len(huc_discharges)
                for i in range(0, pick_count):
                    j = int(i * len(huc_discharges) / pick_count)
                    huc_discharges.append(huc_discharges[j])

            if len(huc_discharges) < 10:
                # Get the sites where the huc is in the huc6
                curs.execute('''
                    SELECT s.site_no, d.peak_discharge, d.min_discharge
                    FROM sites s
                    JOIN discharges d ON s.site_no = d.site_no
                    WHERE s.huc LIKE ? AND d.is_valid = 1
                ''', (f'{huc[:6]}%',))
                sites_with_discharges_huc6 = curs.fetchall()
                print(f'   huc6 sites: {len(sites_with_discharges_huc6)}')
                huc6_discharges = []

                for site in sites_with_discharges_huc6:
                    # site[0] is site_no, site[1] is peak_discharge, site[2] is min_discharge
                    huc6_discharges.append({
                        'site_no': site[0],
                        'peak_discharge': site[1],
                        'min_discharge': site[2]
                    })

                # Sort huc6_discharges by peak discharge
                huc6_discharges.sort(key=lambda x: x['peak_discharge'])
                if len(huc6_discharges) > 0:
                    pick_count = 10 - len(huc_discharges)
                    # pick discharges distributed across the sorted huc6_discharges and add them to huc_discharges
                    for i in range(0, pick_count):
                        j = int(i * len(huc6_discharges) / pick_count)
                        huc_discharges.append(huc6_discharges[j])

            if len(huc_discharges) < 10:
                # grab the huc4
                curs.execute('''
                    SELECT s.site_no, d.peak_discharge, d.min_discharge
                    FROM sites s
                    JOIN discharges d ON s.site_no = d.site_no
                    WHERE s.huc LIKE ? AND d.is_valid = 1
                ''', (f'{huc[:4]}%',))
                sites_with_discharges_huc4 = curs.fetchall()
                print(f'   huc4 sites: {len(sites_with_discharges_huc4)}')
                huc4_discharges = []

                for site in sites_with_discharges_huc4:
                    # site[0] is site_no, site[1] is peak_discharge, site[2] is min_discharge
                    huc4_discharges.append({
                        'site_no': site[0],
                        'peak_discharge': site[1],
                        'min_discharge': site[2]
                    })

                # Sort huc4_discharges by peak discharge
                huc4_discharges.sort(key=lambda x: x['peak_discharge'])

                if len(huc4_discharges) > 0:
                    pick_count = 10 - len(huc_discharges)
                    # pick discharges distributed across the sorted huc4_discharges and add them to huc_discharges
                    for i in range(0, pick_count):
                        j = int(i * len(huc4_discharges) / pick_count)
                        huc_discharges.append(huc4_discharges[j])

            if len(huc_discharges) < 10:
                print(f'   not enough discharges for {huc}')
                continue

            print(f'   processing sites: {len(huc_discharges)}')
            # Sort huc_discharges by peak discharge
            huc_discharges.sort(key=lambda x: x['peak_discharge'])
            # Now pick the median discharge
            median_discharge = huc_discharges[5]['peak_discharge']

            # Resort by min discharge and pick the median min discharge
            huc_discharges.sort(key=lambda x: x['min_discharge'])
            median_min_discharge = huc_discharges[5]['min_discharge']

            print(f'   median discharge: {median_discharge}')
            print(f'   median min discharge: {median_min_discharge}')
            
            curs.execute('''
                INSERT INTO regional_curves
                (
                    huc8,
                    median_peak_discharge,
                    median_min_discharge
                )
                VALUES
                (
                    ?, ?, ?
                )
            ''', (huc, median_discharge, median_min_discharge))
            conn.commit()

            processed += 1

    print(f'hucs processed: {processed}/{count}')


if __name__ == '__main__':
    calculate_regional_curve()
