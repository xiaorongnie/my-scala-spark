package com.transcodegroup.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 *
 * Spark Core快速入门系列(12) | 变量与累加器问题
 *
 * 正常情况下, 传递给 Spark 算子(比如: map, reduce 等)的函数都是在远程的集群节点上执行, 函数中用到的所有变量都是独立的拷贝.
 * 这些变量被拷贝到集群上的每个节点上, 都这些变量的更改不会传递回驱动程序.
 *
 * 支持跨 task 之间共享变量通常是低效的, 但是 Spark 对共享变量也提供了两种支持:
 *    累加器
 *    广播变量
 *
 * @author EASON
 */
object SparkCore_12_Acc {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    // 要在SparkContext初始化之前设置, 都在无效
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    conf.set("dfs.client.use.datanode.hostname", "true")
    val sc = new SparkContext(conf)

    val p1 = Person(10)
    // 将来会把对象序列化之后传递到每个节点上
    val rdd1 = sc.parallelize(List(p1))
    val rdd2: RDD[Person] = rdd1.map(p => {
      p.age = 100;
      p
    })

    rdd2.collect().foreach(println)

    println(rdd2.count())
    // 仍然是 10
    println(p1.age)
  }
}

case class Person(var age:Int)