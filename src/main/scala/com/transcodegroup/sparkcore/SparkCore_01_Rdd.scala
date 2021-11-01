package com.transcodegroup.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 *
 * 在Spark中创建RDD的创建方式可以分为三种
 *
 * @author EASON
 */
object SparkCore_01_Rdd {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //创建conf对象
    val conf = new SparkConf()
      .setAppName("ParallelizeCollection")
      .setMaster("local")
    val sc = new SparkContext(conf)
    parallelize(sc)
    makeRDD(sc)
    textFile(sc)
  }

  /**
   * 通过并行化集合方式创建RDD
   *
   * @param sc
   * @return
   */
  def parallelize(sc: SparkContext): Unit = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //要通过并行化集合方式创建RDD，那么就调用SparkContext以及其子类的parallelize()的方法
    val numberRDD = sc.parallelize(numbers, 5)
    var sum = numberRDD.reduce(_ + _)
    logger.warn("rdd parallelize -> {}", sum)
  }

  /**
   * 使用makeRDD函数创建
   *
   * @param sc
   */
  def makeRDD(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Array(10, 20, 30, 40, 50, 60))
    var sum = rdd1.reduce(_ + _)
    logger.warn("rdd makeRDD -> {}", sum)
  }

  /**
   * 从外部存储创建 RDD
   *
   * @param sc
   */
  def textFile(sc: SparkContext): Unit = {
    var distFile = sc.textFile("./data/words.txt")
    logger.warn("rdd makeRDD -> {}", distFile.collect)
  }

}
