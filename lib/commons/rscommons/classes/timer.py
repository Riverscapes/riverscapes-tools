from __future__ import annotations
from typing import Dict, List, Tuple
import time
import csv
from rsxml.util import pretty_duration


class Timer:

    def __init__(self):
        self._start_time = None
        self._stop_time = None
        self.reset()

    def reset(self):
        self._start_time = time.perf_counter()
        self._stop_time = None

    def toString(self) -> str:
        return pretty_duration(self.ellapsed())

    def ellapsed(self) -> int:
        self._stop_time = time.perf_counter()
        return self._stop_time - self._start_time


class TimerWaypoints:

    def __init__(self):
        self.timer = Timer()
        self.timers = []
        self.total_time = 0

    def timer_break(self, key: str):
        ellapsed = self.timer.ellapsed()
        self.timers.append((key, ellapsed))
        self.total_time += ellapsed
        self.timer.reset()

    def toString(self) -> str:
        return '-----------------------------------\n' \
            + '\n'.join([f'{x}: {round(ell)} seconds' for x, ell in self.timers]) \
            + '\n-----------------------------------\n' \
            + f'total: {round(self.total_time, 3)} seconds' \
            '\n-----------------------------------\n'


class Borg:
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state


class TimerBuckets(Borg):
    timers: Dict(str, float)
    ticks: List(TimerBuckets.Tick)
    """Useful for when you want to time things into buckets. Includes tick function for using within loops

        with TimerBuckets("bucketname"):
            EVERYTHING IN HERE GETS COUNTED IN THIS BUCKET

    """
    class ColTypes:
        UNKNOWN = 'UNKNOWN'
        INT = 'INTEGER'
        REAL = 'REAL'
        TEXT = 'TEXT'

    class Tick:
        timers: Dict(str, float) = {}
        total = 0
        meta = {}

        def __init__(self, timers, total, meta):
            self.timers = timers
            self.total = total
            self.meta = meta

    def __init__(self, key: str = None, table_name: str = None, csv_path: str = None, meta: Dict = None, active: bool = True, reset: bool = False):
        """_summary_

        Args:
            key (str, optional): When using the "with TimerBuckets(key='MyKey')".
            table_name (str, optional): If you want sqlite writes you need this set to a string.
            csv_path (str, optional): Optional. Can be passed during write_csv as well.
            meta (Dict, optional): Metadata key=value pairs dictionary.
            active (bool, optional): if active=false this class won't do anything.
            reset (bool, optional): resets the borg singleton so we can use this in another loop.
        """
        Borg.__init__(self)
        if "timers" not in self.__dict__ or reset is True:
            # Pass in a debug flag to active to prevent anything from happening in this
            self.active = active
            self.table_name = table_name if table_name is not None else 'DEBUG'
            self.timers = {}
            self.meta = {}
            self.total = 0
            self.tick_total = 0
            self.ticks = []

        self.timer = Timer()

        if self.active is False:
            return

        if csv_path is not None:
            self.csv_path = csv_path

        self.key = key
        if meta is not None:
            self.meta = meta

    def tick(self, meta: Dict = {}):
        """ For "for" loops you can call this to freeze these timers to a row

        Args:
            meta (Dict, optional): meta takes the form { "keyname": (value, "SQLITETYPE") }
        """
        if self.active is False:
            return
        # If we only tick when there's something to do then we can tick at the top or the bottom of the loop
        # this is helpful to catch errors partway through the loop
        if self.tick_total > 0 or len(self.timers.keys()) > 0:
            self.ticks.append(TimerBuckets.Tick(self.timers, self.tick_total, self.meta))

        self.meta = meta
        self.timers = {}
        self.tick_total = 0

    def __enter__(self):
        """Behaviour on open when using the "with TimerBuckets():" Syntax
        """
        if self.active is False:
            return
        if self.key is not None:
            if self.key not in self.timers:
                self.timers[self.key] = 0
            self.timer.reset()

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with TimerBuckets():" Syntax
        """
        if self.active is False:
            return
        if self.key is not None:
            self.timers[self.key] += self.timer.ellapsed()
            self.total += self.timer.ellapsed()
            self.tick_total += self.timer.ellapsed()

    def generate_table(self) -> Tuple(List(str, str), List):
        """ return something we can either write to a CSV or to a SQLite DB

        Returns:
            _type_: _description_
        """
        if self.active is False:
            return {}, []

        meta_columns = {"tick": 'INTEGER'}
        tmr_columns = {}
        # First add all the meta keys to the dictionary
        csv_arr = []
        # First go through and find out all the data types
        for idx, row in enumerate(self.ticks):
            csv_row = {"tick": idx}
            for k in row.meta.keys():
                if isinstance(row.meta[k], type(None)):
                    if k not in meta_columns:
                        meta_columns[k] = None
                elif isinstance(row.meta[k], int):
                    if k not in meta_columns or meta_columns[k] is None:
                        meta_columns[k] = TimerBuckets.ColTypes.INT
                elif isinstance(row.meta[k], float):
                    if k not in meta_columns or meta_columns[k] is None:
                        meta_columns[k] = TimerBuckets.ColTypes.REAL
                # Just default back to text for everything else
                else:
                    if k not in meta_columns or meta_columns[k] is None or meta_columns[k] is not TimerBuckets.ColTypes.TEXT:
                        meta_columns[k] = TimerBuckets.ColTypes.TEXT
                    csv_row[k] = row.meta[k]

                csv_row[k] = row.meta[k]

            # Timers get appended as REAL
            for k in row.timers.keys():
                kname = f'tmr_{k}'
                if kname not in tmr_columns:
                    tmr_columns[kname] = 'REAL'
                csv_row[kname] = round(row.timers[k], 1)
            csv_arr.append(csv_row)
            csv_row["timer_total"] = round(row.total, 1)

        columns = [(k, v) for k, v in meta_columns.items()] + \
            [(k, v) for k, v in tmr_columns.items()] + \
            [('timer_total', TimerBuckets.ColTypes.REAL)]

        values = []
        for row in csv_arr:
            row_arr = []
            for col_name, col_type in columns:
                cell_val = row[col_name] if col_name in row else None
                if col_type == TimerBuckets.ColTypes.TEXT and cell_val is not None and not isinstance(cell_val, str):
                    cell_val = str(cell_val)
                row_arr.append(cell_val)
            values.append(row_arr)

        return (columns, values)

    def write_csv(self, csv_file_path: str = None):
        """Write all our Timer ticks to a CSV file

        Args:
            csv_path (str): _description_
        """
        if self.active is False:
            return

        final_path = csv_file_path if csv_file_path is not None else self.csv_path

        if final_path is None:
            return

        columns, csv_arr = self.generate_table()

        with open(final_path, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([k for k, v in columns])  # header row
            for row in csv_arr:
                writer.writerow(row)

    def write_sqlite(self, conn):
        """Write the data to a sqlite file (or geopackage)

        Args:
            conn (_type_): _description_
        """
        if self.active is False or self.table_name is None:
            return

        curs = conn.cursor()
        is_gpkg = curs.execute("SELECT name FROM sqlite_master WHERE type='table' and name='gpkg_contents';").fetchone()

        if is_gpkg:
            has_row = curs.execute("SELECT * from gpkg_contents WHERE table_name=?;", [self.table_name, ]).fetchone()
            if has_row is None:
                curs.execute("""
                INSERT INTO gpkg_contents 
                    (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id) 
                    VALUES 
                    (:table_name, 'attributes', :table_name, '', '2022-11-12T06:14:22.287Z', null, null, null, null, null);
                """, {"table_name": self.table_name})

        columns, csv_arr = self.generate_table()

        # Blow away the old one
        curs.execute(f'DROP TABLE IF EXISTS {self.table_name}')

        create_string = f"""
            CREATE TABLE {self.table_name}
            (
                tick INTEGER not null primary key autoincrement,
                {", ".join([f"{cname} {ctype}" for cname, ctype in columns if cname != 'tick'])}
            )
        """

        curs.execute(create_string)
        value_sub = ','.join(['?' for x in columns])
        curs.executemany(f'INSERT INTO {self.table_name} VALUES({value_sub});', csv_arr)
        conn.commit()
