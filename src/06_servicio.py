import sqlite3,json,pathlib
from pyspark.sql import SparkSession,functions as F
from common import *

def main():
 spark=SparkSession.builder.master('local[2]').appName('SECOP_servicio').getOrCreate()
 df=spark.read.parquet(str(DATA/'curada'))
 cols=['id_registro','fecha_contrato','valor_contrato','departamento','entidad','anio']
 db=sqlite3.connect(DATA/'servicio.db')
 db.execute('CREATE TABLE IF NOT EXISTS contratos(id_registro TEXT PRIMARY KEY,fecha_contrato TEXT,valor_contrato REAL,departamento TEXT,entidad TEXT,anio INTEGER)')
 # UPSERT por id_registro: dos ejecuciones no incrementan los contratos.
 rows=[(r.id_registro,str(r.fecha_contrato),float(r.valor_contrato),r.departamento,r.entidad,int(r.anio)) for r in df.select(*cols).toLocalIterator()]
 db.executemany('INSERT INTO contratos VALUES (?,?,?,?,?,?) ON CONFLICT(id_registro) DO UPDATE SET fecha_contrato=excluded.fecha_contrato, valor_contrato=excluded.valor_contrato,departamento=excluded.departamento,entidad=excluded.entidad,anio=excluded.anio',rows)
 db.executescript("""CREATE TABLE IF NOT EXISTS kpi(nombre TEXT PRIMARY KEY,valor REAL);
 CREATE TABLE IF NOT EXISTS por_departamento(departamento TEXT PRIMARY KEY,contratos INTEGER,total_valor REAL);
 CREATE TABLE IF NOT EXISTS por_anio(anio INTEGER PRIMARY KEY,contratos INTEGER,total_valor REAL);
 CREATE TABLE IF NOT EXISTS por_entidad(entidad TEXT PRIMARY KEY,contratos INTEGER,total_valor REAL);""")
 for t in ['kpi','por_departamento','por_anio','por_entidad']:db.execute('DELETE FROM '+t)
 db.execute("INSERT INTO kpi SELECT 'total_contratos',COUNT(*) FROM contratos")
 db.execute("INSERT INTO kpi SELECT 'valor_total',COALESCE(SUM(valor_contrato),0) FROM contratos")
 db.execute("INSERT INTO kpi SELECT 'total_departamentos',COUNT(DISTINCT departamento) FROM contratos")
 db.execute('INSERT INTO por_departamento SELECT departamento,COUNT(*),SUM(valor_contrato) FROM contratos GROUP BY departamento')
 db.execute('INSERT INTO por_anio SELECT anio,COUNT(*),SUM(valor_contrato) FROM contratos GROUP BY anio')
 db.execute('INSERT INTO por_entidad SELECT entidad,COUNT(*),SUM(valor_contrato) FROM contratos GROUP BY entidad')
 db.commit()
 n=db.execute('SELECT COUNT(*) FROM contratos').fetchone()[0]
 db.close();spark.stop()
 dump(DATA/'reporte_idempotencia.json',{'conteo_en_servicio':n,'politica':'UPSERT por clave primaria id_registro; ejecutar dos veces 06_servicio.py y comparar'})
 print(f'Contratos únicos en SQLite: {n}')
if __name__=='__main__':main()
