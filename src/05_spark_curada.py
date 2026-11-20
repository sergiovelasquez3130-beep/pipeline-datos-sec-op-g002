import os,json,glob,pathlib
from pyspark.sql import SparkSession,functions as F,Window
from common import *

def main():
 spark=SparkSession.builder.master('local[2]').appName('SECOP_curada').config('spark.driver.memory','3g').getOrCreate()
 spark.sparkContext.setLogLevel('WARN')
 path=f'{HDFS_ROOT}/datalake/raw/{SOURCE}/{DATASET}/fecha_ingesta=*/*.jsonl'
 raw=spark.read.json(path)
 count_raw=raw.count()
 cols=['id_contrato','fecha_de_firma','valor_del_contrato','departamento','nombre_entidad','estado_contrato']
 # Se conserva payload íntegro en raw. La selección/normalización solo se aplica a la zona curada.
 payload_cols=raw.select('payload.*').columns
 def pick(*candidates):
  name=next((c for c in candidates if c in payload_cols),None)
  return F.col('payload.`'+name+'`') if name else F.lit(None)
 date=pick('fecha_de_firma','fecha_de_inicio_del_contrato','fecha_de_inicio')
 val=pick('valor_del_contrato','valor_contrato')
 dept=pick('departamento','departamento_entidad','departamento_de_ejecucion')
 ent=pick('nombre_entidad','entidad_estatal','nombre_de_la_entidad')
 df=raw.select(F.col('id_registro').cast('string').alias('id_registro'),F.col('fecha_ingesta'),date.alias('fecha_raw'),val.alias('valor_raw'),dept.alias('departamento_raw'),ent.alias('entidad_raw'),pick('estado_contrato').alias('estado_contrato'))
 df=df.withColumn('fecha_contrato',F.to_date(F.substring('fecha_raw',1,10)))
 df=df.withColumn('valor_contrato',F.regexp_replace(F.col('valor_raw').cast('string'),r'[^0-9.]','').cast('double'))
 df=df.withColumn('departamento',F.coalesce(F.nullif(F.trim('departamento_raw'),F.lit('')),F.lit('SIN INFORMACIÓN')))
 df=df.withColumn('entidad',F.coalesce(F.nullif(F.trim('entidad_raw'),F.lit('')),F.lit('SIN INFORMACIÓN')))
 df=df.withColumn('anio',F.year('fecha_contrato'))
 invalid=df.filter(F.col('id_registro').isNull()|F.col('fecha_contrato').isNull()|F.col('valor_contrato').isNull()|(F.col('valor_contrato')<0))
 invalid_count=invalid.count()
 clean=df.filter(F.col('id_registro').isNotNull()&F.col('fecha_contrato').isNotNull()&F.col('valor_contrato').isNotNull()&(F.col('valor_contrato')>=0))
 # El registro más reciente gana en caso de id duplicado.
 win=Window.partitionBy('id_registro').orderBy(F.col('fecha_ingesta').desc())
 unique=clean.withColumn('_orden',F.row_number().over(win)).filter('_orden=1').drop('_orden')
 unique=unique.select('id_registro','fecha_ingesta','fecha_contrato','valor_contrato','departamento','entidad','estado_contrato','anio')
 unique_count=unique.count()
 if unique_count==0:raise RuntimeError('Validación crítica: zona curada vacía')
 if unique.filter(F.col('id_registro').isNull()).count():raise RuntimeError('Validación crítica: id nulo')
 if unique.groupBy('id_registro').count().filter('count>1').count():raise RuntimeError('Validación crítica: ID duplicado')
 if unique.filter((F.col('valor_contrato')<0)|F.col('fecha_contrato').isNull()).count():raise RuntimeError('Validación crítica: valor/fecha inválidos')
 target=DATA/'curada';target.mkdir(parents=True,exist_ok=True)
 # overwrite mantiene idempotencia al reconstruir la misma muestra; el incremental está en ingesta/servicio.
 unique.write.mode('overwrite').partitionBy('anio').parquet(str(target))
 sum_spark=raw.filter(F.col('id_registro').isNotNull()).count()
 raw_size=sum(f.stat().st_size for f in (DATA/'hdfs_batches').glob('*.jsonl'))
 parquet_size=sum(f.stat().st_size for f in target.rglob('*.parquet'))
 report={'raw_hdfs':count_raw,'spark_mapreduce_equivalente':sum_spark,'invalidos':invalid_count,'duplicados_descartados':count_raw-invalid_count-unique_count,'curados_unicos':unique_count,'validaciones_criticas':'APROBADAS','jsonl_bytes':raw_size,'parquet_bytes':parquet_size,'particion':'anio'}
 dump(DATA/'reporte_calidad.json',report)
 print(json.dumps(report,indent=2));spark.stop()
if __name__=='__main__':main()
