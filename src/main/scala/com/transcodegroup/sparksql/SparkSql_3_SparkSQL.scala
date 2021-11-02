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
object SparkSql_3_SparkSQL {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    // 为样例类创建一个编码器
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .getOrCreate()
    var rdd = sparkSession.sparkContext.wholeTextFiles("./data/people.json").values
    import sparkSession.implicits._
    var df = sparkSession.read.json(rdd.toDS())
    df.show()
    df.printSchema()


    // 注册一个 udf 函数: toUpper是函数名, 第二个参数是函数的具体实现
    sparkSession.udf.register("toUpper", (s: String) => s.toUpperCase)
    df.createOrReplaceTempView("people")
    sparkSession.sql("select toUpper(name), age from people").show

    // 内置聚合函数
    // Spark 系列（十一）—— Spark SQL 聚合函数 Aggregations
    // https://juejin.cn/post/6844903950232059911#heading-12 
    sparkSession.sql("select sum(age) from people").show
  }
}
