package com.transcodegroup.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 *
 ** Spark SQL 快速入门系列(2) | SparkSession与DataFrame的简单介绍
 *
 * @author EASON
 */
object SparkSql_1_DataFrame {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .getOrCreate()
    var df = sparkSession.read.json("./data/fleet.json")
    df.show()

    // 对DataFrame创建一个临时表, 临时视图只能在当前 Session 有效, 在新的 Session 中无效.
    df.createOrReplaceTempView("fleet")
    sparkSession.sql("select * from fleet").show

    // 可以创建全局视图. 访问全局视图需要全路径:如global_temp.xxx
    df.createGlobalTempView("people")
    sparkSession.sql("select * from global_temp.people").show()
    sparkSession.newSession.sql("select * from global_temp.people").show()

    // DSL 语法风格(了解)
    df.printSchema
    df.select("msg").show

    // RDD 和 DataFrame 的交互
    var rdd = df.rdd
    // 从 DataFrame到RDD
    rdd.collect().foreach(println)

    // RDD到DataFrame
    var rdd2 = sparkSession.sparkContext.parallelize(List(("lisi", 10), ("zs", 20), ("zhiling", 40)))
    // 隐式转换
    import sparkSession.implicits._
    rdd2.toDF().show
  }
}
