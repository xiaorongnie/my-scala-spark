package com.transcodegroup

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 *
 * Spark Core快速入门系列(9) | RDD缓存和设置检查点
 *
 * 一. RDD缓存
 * RDD通过persist方法或cache方法可以将前面的计算结果缓存，默认情况下 persist() 会把数据以序列化的形式缓存在 JVM 的堆空间中。
 * 但是并不是这两个方法被调用时立即缓存，而是触发后面的action时，该RDD将会被缓存在计算节点的内存中，并供后面重用。
 *
 * 二. 设置检查点（checkpoint）
 * 对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发, 在触发的时候需要对这个 RDD 重新计算.
 *
 * @author EASON
 */
object SparkCore_07_Checkpoint {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //创建conf对象
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("abc"))
    val nocacheRdd = rdd.map(_.toString + " 不缓存 -> " + System.currentTimeMillis)
    nocacheRdd.collect.foreach(println)
    nocacheRdd.collect.foreach(println)
    nocacheRdd.collect.foreach(println)

    val cacheRdd = rdd.map(_.toString + " 缓存 -> " + System.currentTimeMillis).cache()
    cacheRdd.collect.foreach(println)
    cacheRdd.collect.foreach(println)
    cacheRdd.collect.foreach(println)
  }

}