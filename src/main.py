from db import connect_DB
from data import open_csv
import sys
import csv
import os
import shutil

csv_open = open_csv()
db = connect_DB()
try:
    cursor = db.cursor()
    cursor.execute( "DROP TABLE IF EXISTS localidades")
    cursor.execute("CREATE TABLE IF NOT EXISTS localidades (id int, provincia VARCHAR(255), localidad VARCHAR(255), cp VARCHAR(255), id_prov_mstr VARCHAR(255))")
    for row in csv_open:
        cursor.execute("INSERT INTO localidades (id, provincia, localidad, cp, id_prov_mstr ) VALUES (%s, %s, %s, %s, %s)", (row[1], row[0], row[2], row[3], row[4]))
    db.commit()
except db.Error as e:
    print ('No se pudo crear la tabla. Error: %s' % e)
    db.rollback()
    sys.exit(1)
db.close()

try:
    if not os.path.exists('provincias_csv'):
        os.makedirs('provincias_csv')
    else:
        shutil.rmtree('provincias_csv')
        os.makedirs('provincias_csv')
except OSError as e:
    print ('No se pudo crear la carpeta. Error: %s' % e)
    sys.exit(1)

try:
    db = connect_DB()
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT provincia FROM localidades")
    provincias = cursor.fetchall()
    for provincia in provincias:
        cursor.execute("SELECT * FROM localidades WHERE provincia = %s", (provincia[0],))
        rows = cursor.fetchall()
        with open(f'provincias_csv/{provincia[0]}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
    print('Se agruparon y exportaron las localidades por provincia exitosamente.')
except db.Error as e:
    db.rollback()
    print ('No se pudo agrupar y exportar por provincia. Error: %s' % e)
    sys.exit(1)
db.close()

try:
    db = connect_DB()
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT provincia FROM localidades")
    provincias = cursor.fetchall()
    for provincia in provincias:
        cursor.execute("SELECT COUNT(*) FROM localidades WHERE provincia = %s", (provincia[0],))
        total_rows = cursor.fetchone()
        total_rows = total_rows[0]
        with open(f'provincias_csv/{provincia[0]}.csv', 'r') as f:
            reader = csv.reader(f)
            data = list(reader)
            if len(data) == total_rows:
                print(f'El archivo {provincia[0]}.csv tiene la misma cantidad de registros que la tabla localidades')
            else:
                print(f'El archivo {provincia[0]}.csv no tiene la misma cantidad de registros que la tabla localidades')
    db.close()
except db.Error as e:
    db.rollback()
    print ('No se pudo comprobar la cantidad de registros. Error: %s' % e)
    sys.exit(1)

