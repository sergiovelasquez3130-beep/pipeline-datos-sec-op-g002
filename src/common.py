import os, json, pathlib, datetime, sqlite3
ROOT=pathlib.Path('/project') if pathlib.Path('/project/src').exists() else pathlib.Path(__file__).resolve().parents[1]
DATA=ROOT/'data'
DATA.mkdir(exist_ok=True)
DATASET=os.getenv('DATASET_ID','jbjy-vk9h')
SOURCE='SECOP_II'
API=f'https://www.datos.gov.co/resource/{DATASET}.json'
KAFKA=os.getenv('KAFKA_BOOTSTRAP','localhost:9092')
TOPIC='secop_contratos_raw'
HDFS_URL=os.getenv('HDFS_URL','http://namenode:9870')
HDFS_ROOT=os.getenv('HDFS_ROOT','hdfs://namenode:9000')
def dump(path,obj):
 path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str),encoding='utf8')
def today(): return datetime.datetime.now(datetime.timezone.utc).date().isoformat()
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
