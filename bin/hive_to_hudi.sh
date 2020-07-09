#!/usr/bin/env bash
## hive表转hudi表
params=$1

source /usr/local/spark-2.4.5-bin-hadoop2.7/bin/set-spark-home.sh
spark-submit \
--master yarn \
--deploy-mode cluster \
--files /etc/hive/conf/hive-site.xml \
--num-executors 40 \
--executor-memory 3g \
--driver-memory 6g \
--queue datasync \
--conf spark.scheduler.mode=FAIR \
--conf spark.executor.memoryOverhead=1072 \
--conf spark.driver.memoryOverhead=2048 \
--conf spark.task.cpus=1 \
--conf spark.executor.cores=1 \
--conf spark.task.maxFailures=10 \
--conf spark.memory.fraction=0.4 \
--conf spark.rdd.compress=true \
--conf spark.kryoserializer.buffer.max=200m \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.driver.maxResultSize=3g \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.network.timeout=600s \
--conf spark.yarn.max.executor.failures=10 \
--conf spark.sql.shuffle.partitions=120 \
--conf spark.sql.parquet.writeLegacyFormat=true \
--conf spark.sql.hive.convertMetastoreParquet=false \
--jars hdfs://bgdata/apps/hudi/lib/fastjson-1.2.29.jar \
--class com.company.hudi.ConvertHiveTableToHudi hdfs://bgdata/apps/hudi/lib/hudi-demo-1.0-SNAPSHOT.jar \
--params "${params}" \
