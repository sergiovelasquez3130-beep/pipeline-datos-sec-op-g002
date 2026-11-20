import pathlib,json,sys,sqlite3
from pyspark.sql import SparkSession,functions as F,types as T
from common import *

def main():
 spark=SparkSession.builder.master('local[2]').appName('SECOP_streaming').getOrCreate()
 # Origen permitido por la rúbrica: carpeta de llegada de lotes.
 inbox=DATA/'stream_inbox';inbox.mkdir(exist_ok=True)
 schema=T.StructType([T.StructField('id_registro',T.StringType()),T.StructField('fecha_ingesta',T.StringType())])
 stream=spark.readStream.schema(schema).json(str(inbox)).withColumn('evento',F.to_timestamp('fecha_ingesta'))
 agg=stream.withWatermark('evento','1 day').groupBy(F.window('evento','1 day')).agg(F.count('*').alias('mensajes'))
 def write_batch(df,batchid):
  # UPDATE emite los valores completos de la ventana. REPLACE evita dobles inserciones por retry.
  db=sqlite3.connect(DATA/'servicio.db')
  db.execute('CREATE TABLE IF NOT EXISTS streaming_ventanas(inicio TEXT,fin TEXT,mensajes INTEGER,PRIMARY KEY(inicio,fin))')
  records=[(str(r.window.start),str(r.window.end),int(r.mensajes)) for r in df.collect()]
  db.executemany('INSERT INTO streaming_ventanas VALUES(?,?,?) ON CONFLICT(inicio,fin) DO UPDATE SET mensajes=excluded.mensajes',records)
  db.commit();db.close();print('Microbatch',batchid,'ventanas actualizadas:',len(records))
 q=(agg.writeStream.outputMode('update').foreachBatch(write_batch)
   .option('checkpointLocation',str(DATA/'checkpoints'/'secop_ventanas'))
   .trigger(availableNow=True).start())
 q.awaitTermination()
 with sqlite3.connect(DATA/'servicio.db') as db:total=db.execute('SELECT COALESCE(SUM(mensajes),0) FROM streaming_ventanas').fetchone()[0]
 dump(DATA/'reporte_streaming.json',{'total_mensajes_en_ventanas':total,'checkpoint':str(DATA/'checkpoints'/'secop_ventanas'),'nota':'Volver a ejecutar para comprobar que no se reprocesan archivos ya descubiertos por el checkpoint'})
 print('Total mensajes en ventanas:',total);spark.stop()
if __name__=='__main__':main()
