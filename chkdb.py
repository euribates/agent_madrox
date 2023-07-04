import cx_Oracle
from datetime import date as Date

db = cx_Oracle.connect('agora/agora@oracle')
fecha = Date(2023, 5, 1)
sql = 'Select id_noticia as pk from Noticias.Noticia WHERE f_alta >= :1'
parameters = [fecha]
cur = db.cursor()
cur.execute(sql, parameters)
for row in cur.fetchall():
    print(row)

import dba
dbc = dba.get_database_connection('DB_SOURCE')
for row in dba.get_rows(dbc, sql, fecha):
    print(row)
