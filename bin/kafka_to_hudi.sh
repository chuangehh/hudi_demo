#!/usr/bin/env bash
## kafka数据流入hudi
hudiTableName=$1
tableType=$2
recordkeyField=$3
partitionpathField=$4
precombineField=$5
targetDatabase="ods"

source /usr/local/spark-2.4.5-bin-hadoop2.7/bin/set-spark-home.sh

spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --files /etc/hive/conf/hive-site.xml \
 --num-executors 1 \
 --executor-memory 3g \
 --driver-memory 3g \
 --queue datasync \
 --name "${hudiTableName}" \
 --conf spark.scheduler.mode=FAIR \
 --conf spark.executor.memoryOverhead=1072 \
 --conf spark.driver.memoryOverhead=1072 \
 --conf spark.task.cpus=1 \
 --conf spark.executor.cores=1 \
 --conf spark.task.maxFailures=10 \
 --conf spark.memory.fraction=0.4 \
 --conf spark.rdd.compress=true \
 --conf spark.kryoserializer.buffer.max=200m \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 --conf spark.memory.storageFraction=0.1 \
 --conf spark.driver.maxResultSize=3g \
 --conf spark.executor.heartbeatInterval=120s \
 --conf spark.network.timeout=600s \
 --conf spark.yarn.max.executor.failures=10 \
 --conf spark.sql.shuffle.partitions=120 \
 --conf spark.shuffle.service.enabled=true \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.minExecutors=1 \
 --conf spark.dynamicAllocation.maxExecutors=5 \
 --conf spark.dynamicAllocation.executorIdleTimeout=300 \
 --conf spark.sql.parquet.writeLegacyFormat=true \
 --conf spark.sql.hive.convertMetastoreParquet=false \
 --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer hdfs://bgdata/apps/spark_245_jars/hudi-utilities-bundle_2.11-0.5.2-incubating.jar \
 --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
 --source-ordering-field "${precombineField}"  \
 --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
 --target-base-path "/apps/hive/warehouse/${targetDatabase}.db/${hudiTableName}" \
 --target-table "${hudiTableName}" \
 --table-type "${tableType}" \
 --enable-hive-sync \
 --hoodie-conf auto.offset.reset=earliest \
 --hoodie-conf schema.registry.url=http://bgnode17:8081 \
 --hoodie-conf bootstrap.servers=bgnode10:6667,bgnode11:6667,bgnode12:6667 \
 --hoodie-conf hoodie.deltastreamer.source.kafka.topic="${targetDatabase}.${hudiTableName}" \
 --hoodie-conf hoodie.deltastreamer.schemaprovider.registry.url="http://bgnode17:8081/subjects/${targetDatabase}.${hudiTableName}/versions/latest" \
 --hoodie-conf hoodie.datasource.write.recordkey.field="${recordkeyField}" \
 --hoodie-conf hoodie.datasource.write.partitionpath.field="${partitionpathField}" \
 --hoodie-conf hoodie.datasource.write.precombine.field="${precombineField}" \
 --hoodie-conf hoodie.parquet.compression.codec=snappy \
 --hoodie-conf hoodie.parquet.compression.ratio=0.4 \
 --hoodie-conf hoodie.upsert.shuffle.parallelism=30 \
 --hoodie-conf hoodie.insert.shuffle.parallelism=30 \
 --hoodie-conf hoodie.bulkinsert.shuffle.parallelism=30 \
 --hoodie-conf hoodie.index.type=GLOBAL_BLOOM \
 --hoodie-conf hoodie.datasource.hive_sync.database="${targetDatabase}" \
 --hoodie-conf hoodie.datasource.hive_sync.table="${hudiTableName}" \
 --hoodie-conf hoodie.datasource.hive_sync.username=liangchuanchuan \
 --hoodie-conf hoodie.datasource.hive_sync.password=liangchuanchuan \
 --hoodie-conf hoodie.datasource.hive_sync.jdbcurl="jdbc:hive2://bgnode3:10000" \
 --hoodie-conf hoodie.datasource.hive_sync.partition_fields="${partitionpathField}" \
 --hoodie-conf hoodie.datasource.hive_sync.use_pre_apache_input_format=true \
 --hoodie-conf hoodie.datasource.hive_sync.partition_extractor_class="org.apache.hudi.hive.MultiPartKeysValueExtractor" \
 --hoodie-conf hoodie.embed.timeline.server=true
