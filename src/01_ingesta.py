import os, json, time, hashlib, pathlib, sys
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from common import *

def main():
 n=int(os.getenv('TARGET_RECORDS','30000'))
 if n<20000: raise ValueError('La rúbrica exige al menos 20.000 registros')
 date=today(); out=DATA/'raw'/SOURCE/DATASET/f'fecha_ingesta={date}'/'contratos.jsonl'
 out.parent.mkdir(parents=True,exist_ok=True)
 # Captura reproducible por fecha de firma; ordenar por fecha + id reduce cambios entre páginas.
 # El parámetro fecha_de_firma no debe ser nulo para calcular indicadores temporales.
 base="fecha_de_firma IS NOT NULL AND id_contrato IS NOT NULL"
 s=requests.Session();s.mount('https://',HTTPAdapter(max_retries=Retry(total=5,backoff_factor=1,status_forcelist=[429,500,502,503,504])))
 api_params={'$where':base,'$order':'fecha_de_firma ASC, id_contrato ASC'}
 count=s.get(API,params={'$select':'count(*)','$where':base},timeout=90);count.raise_for_status()
 available=int(count.json()[0]['count'])
 if available < n: raise RuntimeError(f'Solo hay {available} registros: se solicitaron {n}')
 total=0; ids=set();duplicates=0; pages=0; batch=1000
 tmp=out.with_suffix('.tmp')
 with tmp.open('w',encoding='utf-8') as f:
  while total<n:
   params={**api_params,'$limit':min(batch,n-total),'$offset':total}
   r=s.get(API,params=params,timeout=120);r.raise_for_status()
   rows=r.json()
   if not rows: raise RuntimeError(f'API devolvió página vacía en offset {total}')
   for payload in rows:
    ident=str(payload['id_contrato'])
    if ident in ids: duplicates+=1
    ids.add(ident)
    envelope={'fuente':SOURCE,'dataset':DATASET,'id_registro':ident,'fecha_ingesta':now(),'payload':payload}
    f.write(json.dumps(envelope,ensure_ascii=False)+'\n')
   total+=len(rows);pages+=1
   print(f'Ingesta {total}/{n}')
 tmp.replace(out)
 manifest={'fuente':SOURCE,'dataset':DATASET,'url':API,'parametros':api_params,'tamano_pagina':batch,'paginas':pages,'disponibles_en_api':available,'registros_descargados':total,'ids_unicos_en_lote':len(ids),'ids_duplicados_en_lote':duplicates,'fecha_ingesta':date,'archivo':str(out.relative_to(ROOT)),'sha256':hashlib.sha256(out.read_bytes()).hexdigest()}
 dump(DATA/'manifiesto.json',manifest)
 print(json.dumps(manifest,indent=2,ensure_ascii=False))
if __name__=='__main__':main()
