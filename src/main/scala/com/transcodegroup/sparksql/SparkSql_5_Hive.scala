package com.transcodegroup.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 *
 * * Spark SQL 快速入门系列(6) | 一文教你如何自定义 SparkSQL 函数
 *
 * @author EASON
 */
object SparkSql_5_Hive {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport() //开启对hive的支持
      .getOrCreate()

    spark.sql("show databases").show();
    //2.1 创建一张hive表
    spark.sql("create table people(id string,name string,age int) row format delimited fields terminated by ','")
    spark.sql("select * from people").show()
  }
}
