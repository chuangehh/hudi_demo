package com.company.hudi

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.company.avro.ParameterTool
import com.company.utils.HttpUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 获取hudi schema
 *
 * schema.registry.url: 注册schema地址
 * hudiBasePath: 非必填,hudi的hdfs路径默认/apps/hive/warehouse/hudi.db
 * hudiTableName: 必填,hudi表名
 * schema.registry.url: 注册schema地址
 */
object RegistryHudiAvroSchema {

  val hudiPrefix = "ods"

  def main(args: Array[String]): Unit = {
    // 0.获取参数
    val params = ParameterTool.fromArgs(args);
    val schemaRegistryUrl = params.getOrDefault("schema.registry.url", "http://bgnode17:8081")
    val hudiTableName = params.get("hudiTableName")
    val hiveSQL = params.get("hiveSQL")

    // 1.获取schema
    val avroSchema = getAvroSchemaBySpark(hiveSQL, hudiTableName);
    // val avroSchema = "{\"type\":\"record\",\"name\":\"sellercube_productcustomerlabel_avro\",\"fields\":[{\"name\":\"hrowkey\",\"type\":[\"string\",\"null\"]},{\"name\":\"labelid\",\"type\":[\"long\",\"null\"]},{\"name\":\"customerlabel\",\"type\":[\"string\",\"null\"]},{\"name\":\"keywords\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"productcode\",\"type\":[\"string\",\"null\"]},{\"name\":\"productid\",\"type\":[\"long\",\"null\"]},{\"name\":\"propertyid\",\"type\":[\"long\",\"null\"]},{\"name\":\"language\",\"type\":[\"string\",\"null\"]},{\"name\":\"enabled\",\"type\":[\"int\",\"null\"]},{\"name\":\"adduser\",\"type\":[\"string\",\"null\"]},{\"name\":\"adddatetime\",\"type\":[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"autochangeprice\",\"type\":[\"int\",\"null\"]},{\"name\":\"upc\",\"type\":[\"string\",\"null\"]},{\"name\":\"amazoneuser\",\"type\":[\"string\",\"null\"]},{\"name\":\"amazonetitle\",\"type\":[\"string\",\"null\"]},{\"name\":\"amazoneprice\",\"type\":[{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"sellercube_productcustomerlabel_avro.amazoneprice\",\"size\":13,\"logicalType\":\"decimal\",\"precision\":30,\"scale\":10},\"null\"]},{\"name\":\"amazonisnotonline\",\"type\":[\"string\",\"null\"]},{\"name\":\"smtisnotonline\",\"type\":[\"long\",\"null\"]},{\"name\":\"amazonshiparea\",\"type\":[\"long\",\"null\"]},{\"name\":\"lastsynctime\",\"type\":[\"long\",\"null\"]},{\"name\":\"modifytimestamp\",\"type\":[\"long\",\"null\"]},{\"name\":\"isdeleted\",\"type\":[\"int\",\"null\"]},{\"name\":\"deleteduserid\",\"type\":[\"long\",\"null\"]},{\"name\":\"deletedtime\",\"type\":[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"record_timemillis\",\"type\":[\"long\",\"null\"]},{\"name\":\"sysevent\",\"type\":[\"string\",\"null\"]},{\"name\":\"hudi_pt\",\"type\":[\"string\",\"null\"]}]}"

    // 2.注册schema
    registrySchema(schemaRegistryUrl, hudiPrefix, hudiTableName, avroSchema)
  }

  def removeHudiFields(avroSchemaStr: String): String = {
    val avroSchema = JSON.parse(avroSchemaStr).asInstanceOf[JSONObject]
    val iterator = avroSchema.getJSONArray("fields").iterator()
    while (iterator.hasNext) {
      val field = iterator.next().asInstanceOf[JSONObject]

      // 1.过滤hudi 相关字段
      val fieldName = field.getString("name")
      if (fieldName.startsWith("_hoodie")) {
        iterator.remove()
      } else {
        // 2.null 与其他类型换位置, 兼容streamsets
        val fieldType = field.getJSONArray("type")
        val fieldType0 = fieldType.get(0)
        fieldType.set(0, fieldType.get(1))
        fieldType.set(1, fieldType0)

        if (fieldType0.isInstanceOf[JSONObject] &&
          fieldType0.asInstanceOf[JSONObject].get("logicalType").equals("timestamp-micros")) {
          fieldType0.asInstanceOf[JSONObject].put("logicalType", "timestamp-millis")
        }
      }
    }

    val result = avroSchema.toString
    println(result)

    result
  }


  /**
   * 注册schema到注册中心
   *
   * @param schemaRegistryUrl
   * @param hudiTableName
   * @param hudiAvroSchema
   */
  def registrySchema(schemaRegistryUrl: String, hudiPrefix: String, hudiTableName: String, hudiAvroSchema: String): Unit = {
    val avroSchema = removeHudiFields(hudiAvroSchema)
    val json = new JSONObject()
    json.put("schema", avroSchema)
    val subjectSchemaStr = json.toString;
    val subjectsUrl = s"$schemaRegistryUrl/subjects/$hudiPrefix.$hudiTableName/versions"
    println("注册中心 subjectsUrl: " + subjectsUrl)
    println("注册中心 schema: " + subjectSchemaStr)
    HttpUtils.sendPost(subjectsUrl, subjectSchemaStr)

    val getSubjectsUrl = s"$schemaRegistryUrl/subjects/$hudiPrefix.$hudiTableName/versions/latest"
    println("注册中心 getSubjectsUrl: " + getSubjectsUrl)
    val result = JSON.parse(HttpUtils.sendGet(getSubjectsUrl, "1=1")).asInstanceOf[JSONObject]

    if (JSON.parse(result.getString("schema")).toString.equals(avroSchema)) {
      println("注册成功")
    } else {
      println("注册失败")
      System.exit(1)
    }
  }

  /**
   * 根据spark获取schema
   *
   * @param hiveSQL
   * @return
   */
  def getAvroSchemaBySpark(hiveSQL: String, hudiTableName: String): String = {
    // spark环境变量
    val sparkConf = new SparkConf()
    val spark = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // 转换为avro schema
    import org.apache.spark.sql.avro.SchemaConverters
    val sparkSchema = spark.sql(hiveSQL).schema
    val avroSchema = SchemaConverters.toAvroType(sparkSchema, nullable = false, hudiTableName)

    println("sparkSchema: " + sparkSchema.json)
    println("\navroSchema: " + avroSchema)

    avroSchema.toString
  }

}
