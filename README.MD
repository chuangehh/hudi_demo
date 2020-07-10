# hudi一键入湖V1.0

## 1.全量同步
* 1.1 数据库 -> spark -> hudi
   * 1.1.1 数据库 -> spark
   * 1.1.2 注册临时表,注册schema
   * 1.1.3 计算同步字段（updatetime,修改时间） max值，开个新库
   * 1.1.4 spark -> hudi

## 2.增量采集
* 1.1 注册kafka topic
* 1.2 配置streamsets,并启动
    * 1.2.1 配置公共值
    * 1.2.2 JDBC
    * 1.2.3 Field Type 
    * 1.2.4 Kafka
* 1.3 调用定时任务默认1小时一次 kafka_to_hudi.sh
