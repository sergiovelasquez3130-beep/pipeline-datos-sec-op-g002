import os,subprocess,json
from common import *
# Ejecutar dentro del contenedor namenode con Hadoop Streaming (3.2.1).
def main():
 report=json.loads((DATA/'reporte_hdfs.json').read_text())
 date=json.loads((DATA/'manifiesto.json').read_text())['fecha_ingesta']
 src=f'/datalake/raw/{SOURCE}/{DATASET}/fecha_ingesta={date}'
 # El jar se descarga en el host desde Maven si la imagen no lo incorpora.
 subprocess.run(['bash','src/run_mapreduce.sh',src],check=True,cwd=ROOT)
if __name__=='__main__':main()
