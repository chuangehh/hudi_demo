#!/bin/bash

## 1 手动: hive -> hudi
sh hive_to_hudi.sh \
'{"targetTable":"${targetTable}","tableType":"${tableType}","recordkey.field":"${recordkey.field}","partitionpath.field":"${partitionpath.field}","precombine.field":"${precombine.field}","sourceDataBase":"ods_test","sourceTable":"${targetTable}","derivativePartition":"${derivativePartition} as ${partitionpath.field}"}'

## 2.手动: 注册schema
sh registry_hudi_schema.sh \
"select * from hudi.${targetTable}" \
${targetTable}

## 3.创建topic,配置流任务

## 4.自动: 配置定时任务执行merge
sh kafka_to_hudi.sh \
${targetTable} \
${tableType} \
${recordkey.field} ${partitionpath.field} ${precombine.field}