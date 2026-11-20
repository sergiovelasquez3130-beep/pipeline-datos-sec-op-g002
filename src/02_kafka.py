import json, time, pathlib, os
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from common import *

def main():
 admin=KafkaAdminClient(bootstrap_servers=KAFKA,client_id='secop_admin',api_version_auto_timeout_ms=20000)
 if TOPIC not in admin.list_topics(): admin.create_topics([NewTopic(name=TOPIC,num_partitions=3,replication_factor=1)])
 admin.close()
 file=pathlib.Path(os.getenv('RAW_FILE') or json.loads((DATA/'manifiesto.json').read_text())['archivo']);file=file if file.is_absolute() else ROOT/file
 producer=KafkaProducer(bootstrap_servers=KAFKA,acks='all',retries=6,key_serializer=lambda x:x.encode(),value_serializer=lambda x:x.encode(),max_request_size=10000000)
 sent=0
 with file.open(encoding='utf8') as f:
  for line in f:
   rec=json.loads(line);producer.send(TOPIC,key=rec['id_registro'],value=line.rstrip('\n'));sent+=1
 producer.flush();producer.close()
 print(f'Kafka: {sent} mensajes enviados al tópico {TOPIC}. Reejecutar el productor crea duplicados en Kafka; la zona curada elimina por id_registro.')
if __name__=='__main__':main()
