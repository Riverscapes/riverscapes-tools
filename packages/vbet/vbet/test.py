import sqlite3

db = '/mnt/c/Users/jordang/Documents/Riverscapes/data/vbet/16010202/inputs/vbet_inputs.gpkg'

conn = sqlite3.connect(db)

curs = conn.cursor()

transformtype = curs.execute("""SELECT transform_types.name from transforms INNER JOIN transform_types ON transform_types.type_id = transforms.type_id where transforms.transform_id = ?""", [27]).fetchone()[0]

print(transformtype)
