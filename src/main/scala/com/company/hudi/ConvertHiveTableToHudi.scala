package com.company.hudi

import com.alibaba.fastjson.{JSON, JSONObject}
import com.company.avro.ParameterTool
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType

/**
 * hive表转hudi表
 *
 * hiveSQL: 必填,查询hive的SQL
 * hudiBasePath: 非必填,hudi的hdfs路径默认/apps/hive/warehouse/hudi.db
 * hudiTableName: 必填,hudi表名
 *
 * recordkey.field: 必填,唯一列
 * partitionpath.field: 必填,分区列
 * precombine.field: 必填,排序列
 */
object ConvertHiveTableToHudi {

  def convertSelectFieldBySql(spark: SparkSession, sourceDataBase: String, sourceTable: String, derivativePartition: String): String = {
    val iterator = spark.sql(s"select * from $sourceDataBase.$sourceTable").schema.fields.iterator
    val sb = new StringBuilder()
    while (iterator.hasNext) {
      val field = iterator.next()

      // 1.过滤hudi 相关字段
      val dataType = field.dataType
      if (dataType.isInstanceOf[TimestampType]) {
        sb.append(s"cast(${field.name} as string) as ${field.name},\n")
      } else {
        sb.append(field.name).append(",\n")
      }
    }

    var selectFields = "";
    if (derivativePartition != null && !derivativePartition.trim.equals("")) {
      selectFields = sb.toString() + derivativePartition
    } else {
      selectFields = sb.substring(0, sb.length - 2)
    }
    s"select $selectFields from $sourceDataBase.$sourceTable"
  }

  /**
   * val params = JSON.parse("{\"targetTable\":\"sellercube_dbo_productstockchangelog_ct\",\"tableType\":\"COPY_ON_WRITE\",\"recordkey.field\":\"primaryid\",\"partitionpath.field\":\"hudi_pt\",\"precombine.field\":\"primaryid\",\"sourceDataBase\":\"ods_test\",\"sourceTable\":\"sellercube_dbo_productstockchangelog_ct\",\"derivativePartition\":\"substr(updatetime,1,7) as hudi_pt\"}").asInstanceOf[JSONObject]
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 获取参数
    val params = JSON.parse(ParameterTool.fromArgs(args).get("params")).asInstanceOf[JSONObject]
    println(s"params:\n$params")
    val hudiBasePath = params.getOrDefault("hudiBasePath", "/apps/hive/warehouse/ods.db").asInstanceOf[String]
    val sourceDataBase = params.getString("sourceDataBase")
    val sourceTable = params.getString("sourceTable")

    val targetDatabase = params.getOrDefault("targetDatabase", "ods").asInstanceOf[String]
    val targetTable = params.getString("targetTable")
    val targetPath = s"$hudiBasePath/$targetTable"
    val tableType = params.getString("tableType")
    val recordkeyField = params.getString("recordkey.field")
    val partitionpathField = params.getString("partitionpath.field")
    val precombineField = params.getString("precombine.field")
    val derivativePartition = params.getOrDefault("derivativePartition", "").asInstanceOf[String]

    // spark环境变量
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder.appName(targetTable).config(sparkConf).enableHiveSupport().getOrCreate()

    val hiveSQL = convertSelectFieldBySql(spark, sourceDataBase, sourceTable, derivativePartition)
    print("\nhiveSQL:\n" + hiveSQL)

    spark.sql(hiveSQL).
      write.format("org.apache.hudi").
      option("hoodie.index.type", "GLOBAL_BLOOM").
      option("hoodie.datasource.write.commitmeta.key.prefix", "deltastreamer.checkpoint.key").
      option("deltastreamer.checkpoint.key", s"${targetDatabase}.${targetTable},0:0").
      option("hoodie.datasource.write.table.type", tableType).
      option("hoodie.datasource.write.operation", "bulk_insert").
      option("hoodie.parquet.compression.ratio", "0.4").
      option("hoodie.parquet.compression.codec", "snappy").
      option("hoodie.datasource.write.recordkey.field", recordkeyField).
      option("hoodie.datasource.write.partitionpath.field", partitionpathField).
      option("hoodie.datasource.write.precombine.field", precombineField).
      option("hoodie.table.name", targetTable).
      option("hoodie.upsert.shuffle.parallelism", 80).
      option("hoodie.insert.shuffle.parallelism", 80).
      option("hoodie.bulkinsert.shuffle.parallelism", 80).
      option("hoodie.datasource.hive_sync.enable", "true").
      option("hoodie.datasource.hive_sync.database", targetDatabase).
      option("hoodie.datasource.hive_sync.table", targetTable).
      option("hoodie.datasource.hive_sync.username", "liangchuanchuan").
      option("hoodie.datasource.hive_sync.password", "liangchuanchuan").
      option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://bgnode3:10000").
      option("hoodie.datasource.hive_sync.partition_fields", partitionpathField).
      option("hoodie.datasource.hive_sync.use_pre_apache_input_format", "true").
      option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor").
      mode("Overwrite").
      save(targetPath)
  }

}
