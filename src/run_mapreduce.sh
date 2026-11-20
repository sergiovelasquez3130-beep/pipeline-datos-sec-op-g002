#!/usr/bin/env bash
set -euo pipefail
INPUT="$1"
mkdir -p evidencias
# Transferencia de scripts y jar al NameNode. El jar debe corresponder a Hadoop 3.2.1.
if [ ! -f data/hadoop-streaming-3.2.1.jar ]; then
 curl -fL --retry 4 'https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-streaming/3.2.1/hadoop-streaming-3.2.1.jar' -o data/hadoop-streaming-3.2.1.jar
fi
docker cp data/hadoop-streaming-3.2.1.jar "$(docker compose ps -q namenode)":/tmp/hadoop-streaming.jar
docker cp src/mapper.py "$(docker compose ps -q namenode)":/tmp/mapper.py
docker cp src/reducer.py "$(docker compose ps -q namenode)":/tmp/reducer.py
docker compose exec -T namenode hdfs dfs -rm -r -f /datalake/mapreduce_out >/dev/null 2>&1 || true
docker compose exec -T namenode hadoop jar /tmp/hadoop-streaming.jar -D mapreduce.job.reduces=1 -input "$INPUT" -output /datalake/mapreduce_out -mapper 'python /tmp/mapper.py' -reducer 'python /tmp/reducer.py' -file /tmp/mapper.py -file /tmp/reducer.py
docker compose exec -T namenode hdfs dfs -cat '/datalake/mapreduce_out/part-*' | tee evidencias/mapreduce_conteo.txt
