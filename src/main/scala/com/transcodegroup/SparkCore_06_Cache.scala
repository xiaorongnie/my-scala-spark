package com.transcodegroup

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 *
 * Spark Core快速入门系列(8) | RDD 的持久化
 *
 * 每碰到一个 Action 就会产生一个 job, 每个 job 开始计算的时候总是从这个 job 最开始的 RDD 开始计算.
 *
 * @author EASON
 */
object SparkCore_06_Cache {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //创建conf对象
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List("ab", "bc"), 2)
    val rdd2 = rdd1.flatMap(x => {
      println("flatMap...")
      x.split("")
      // ***cache是重点***
    }).cache()

    val rdd3: RDD[(String, Int)] = rdd2.map(x => {
      (x, 1)
    })

    /**
     * 每调用一次 collect, 都会创建一个新的 job, 每个 job 总是从它血缘的起始开始计算. 所以, 会发现中间的这些计算过程都会重复的执行.
     * 原因是因为 rdd记录了整个计算过程. 如果计算的过程中出现哪个分区的数据损坏或丢失, 则可以从头开始计算来达到容错的目的.
     * .cache()设置会调用persist(MEMORY_ONLY)，但是，语句执行到这里，并不会缓存rdd，因为这时rdd还没被计算生成
     */
    println("1.-----------")
    rdd3.collect.foreach(println)
    println("2.-----------")
    rdd3.collect.foreach(println)


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