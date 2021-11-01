package com.transcodegroup.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 *
 * * Spark SQL 快速入门系列(3) | DataSet的简单介绍及与DataFrame的交互
 *
 * @author EASON
 */
object SparkSql_2_DataSet {

  val logger = LoggerFactory.getLogger(this.getClass)

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    // 为样例类创建一个编码器
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .getOrCreate()
    import sparkSession.implicits._
    // 使用样例类的序列得到DataSet
    val ds = Seq(Person("lisi", 20), Person("zs", 21)).toDS
    ds.show
    // 使用基本类型的序列得到 DataSet
    val ds2 = Seq(1, 2, 3, 4, 5, 6).toDS
    ds2.show
    // 从 RDD 到 DataSet
    var rdd = sparkSession.sparkContext.parallelize(List(("lisi", 10), ("zs", 20), ("zhiling", 40)))
    rdd.toDS().show()
    // 从 DataSet 到 RDD
    ds2.rdd.collect().foreach(println)

    // 从 DataFrame到DataSet
    var df = rdd.toDF("name", "age")
    df.show
    var dataSet = df.as[Person]
    dataSet.show

    // 从 DataSet到DataFrame
    val dsN = Seq(Person("Andy", 32)).toDS()
    dsN.toDF().show()
  }
}
