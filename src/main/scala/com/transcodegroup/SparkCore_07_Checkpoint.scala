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

    // 要在SparkContext初始化之前设置, 都在无效
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 设置 checkpoint的目录. 如果spark运行在集群上, 则必须是 hdfs 目录
    sc.setCheckpointDir("hdfs://192.168.0.230:9000/ck2")
    val rdd1 = sc.parallelize(List("abc"))
    val rdd2: RDD[String] = rdd1.map(_ + " : " + System.currentTimeMillis())

    /*
    标记 RDD2的 checkpoint.
    RDD2会被保存到文件中(文件位于前面设置的目录中), 并且会切断到父RDD的引用, 也就是切断了它向上的血缘关系
    该函数必须在job被执行之前调用.
    强烈建议把这个RDD序列化到内存中, 否则, 把他保存到文件的时候需要重新计算.
     */
    rdd2.checkpoint()
    rdd2.collect().foreach(println)
    rdd2.collect().foreach(println)
    rdd2.collect().foreach(println)
  }
}