package com.transcodegroup.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 *
 * * Spark SQL 快速入门系列(6) | 一文教你如何自定义 SparkSQL 函数
 *
 * @author EASON
 */
object SparkSql_4_LoadSave {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .getOrCreate()
    import sparkSession.implicits._
    // 保存HDFS
    val df = (1 to 10).toDF("num")
    df.coalesce(1).write.format("json").mode("overwrite").save("hdfs://namenode:9000/SparkSql_4_LoadSave")

    df.repartition(2)
      .write
      .parquet("E:\\DOITLearning\\12.Spark\\parquet_out2")
  }
}
