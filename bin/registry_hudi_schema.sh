#!/usr/bin/env bash
hiveSQL=$1
hudiTableName=$2

source /usr/local/spark-2.4.5-bin-hadoop2.7/bin/set-spark-home.sh
spark-submit \
--jars hdfs://bgdata/apps/hudi/lib/fastjson-1.2.29.jar \
--class com.company.hudi.RegistryHudiAvroSchema hdfs://bgdata/apps/hudi/lib/hudi-demo-1.0-SNAPSHOT.jar \
--hiveSQL "$hiveSQL" \
--hudiTableName $hudiTableName \
--schema.registry.url "http://bgnode17:8081"