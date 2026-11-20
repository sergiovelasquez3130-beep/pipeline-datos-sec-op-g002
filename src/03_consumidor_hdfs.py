import json, pathlib, time, os
import requests
from kafka import KafkaConsumer, TopicPartition
from common import *

def webhdfs(path,method='GET',**kwargs):
 url=f'{HDFS_URL}/webhdfs/v1{path}'
 r=requests.request(method,url,timeout=120,allow_redirects=True,**kwargs)
 r.raise_for_status(); return r

def mkdir(path): webhdfs(path,'PUT',params={'op':'MKDIRS'})
def upload(path,local):
 # WebHDFS CREATE redirige al DataNode. En la red Docker el hostname datanode es resoluble.
 r=requests.put(f'{HDFS_URL}/webhdfs/v1{path}',params={'op':'CREATE','overwrite':'true'},allow_redirects=False,timeout=60);r.raise_for_status()
 if r.status_code!=307: raise RuntimeError(f'CREATE esperaba 307, obtuvo {r.status_code}')
 with open(local,'rb') as f: requests.put(r.headers['Location'],data=f,timeout=300).raise_for_status()

def main():
 manifest=json.loads((DATA/'manifiesto.json').read_text(encoding='utf8'))
 expected=manifest['registros_descargados']; date=manifest['fecha_ingesta']
 state=DATA/'consumer_offsets.json';previous=json.loads(state.read_text()) if state.exists() else {}
 consumer=KafkaConsumer(bootstrap_servers=KAFKA,group_id='secop_hdfs_v1',enable_auto_commit=False,auto_offset_reset='earliest',consumer_timeout_ms=6000,value_deserializer=lambda x:x.decode('utf8'))
 parts=[TopicPartition(TOPIC,i) for i in range(3)];consumer.assign(parts)
 # Reproducible: usa el rango completo de Kafka, incluidos reenvíos anteriores.
 consumer.seek_to_beginning(*parts)
 end=consumer.end_offsets(parts)
 outdir=DATA/'hdfs_batches';outdir.mkdir(exist_ok=True)
 batch=[];seen=0;locations=[];k=0
 def flush():
  nonlocal k,batch
  if not batch:return
  k+=1; local=outdir/f'lote_{k:04d}.jsonl';local.write_text('\n'.join(batch)+'\n',encoding='utf8')
  hdir=f'/datalake/raw/{SOURCE}/{DATASET}/fecha_ingesta={date}'
  mkdir(hdir);dest=f'{hdir}/lote_{k:04d}.jsonl'; upload(dest,local);locations.append(dest);batch=[]
 try:
  # Solo se ejecuta hasta los offsets capturados al comienzo para no quedar en escucha infinita.
  while True:
   if all(consumer.position(tp)>=end[tp] for tp in parts):break
   records=consumer.poll(timeout_ms=3000,max_records=500)
   if not records:continue
   for tp,msgs in records.items():
    for m in msgs:
     if m.offset>=end[tp]:continue
     batch.append(m.value);seen+=1
     if len(batch)>=1000:flush()
  flush()
 finally:consumer.close()
 dump(DATA/'reporte_hdfs.json',{'mensajes_leidos':seen,'lotes':len(locations),'ubicaciones':locations,'registros_de_ingesta':expected})
 print(f'HDFS: {seen} mensajes en {len(locations)} lotes. Posibles repetidos se depuran en Spark.')
if __name__=='__main__':main()
