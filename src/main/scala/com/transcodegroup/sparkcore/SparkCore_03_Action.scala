package com.transcodegroup.sparkcore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 *
 * 在Spark中创建RDD的创建方式可以分为三种
 *
 * @author EASON
 */
object SparkCore_03_Action {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //创建conf对象
    val conf = new SparkConf()
      .setAppName("SparkCore_03_Action")
      .setMaster("local")
    val sc = new SparkContext(conf)
    //  reduce 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。
    reduceOpt(sc)
    // collect 在驱动程序中，以数组的形式返回数据集的所有元素。
    collectOpt(sc)
  }

  /**
   * reduce 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。
   *
   * @param sc
   * @return
   */
  def reduceOpt(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(1 to 10, 2)
    logger.warn("reduceOpt -> {}", rdd1.reduce(_ + _))

    val rdd2 = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))
    logger.warn("reduceOpt -> {}", rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2)))
  }

  /**
   * collect 在驱动程序中，以数组的形式返回数据集的所有元素。
   *
   * @param sc
   * @return
   */
  def collectOpt(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(1 to 10, 2)
    logger.warn("collect -> {} count-> {}", rdd.collect(), rdd.count)
    logger.warn("first -> {} take 1-> {}", rdd.first, rdd.take(3))
    // 将该RDD所有元素相加得到结果
    logger.warn("aggregate -> {}", rdd.aggregate(0)(_ + _, _ + _))
    // fold aggregate的简化操作，seqop和combop一样的时候,可以使用fold
    logger.warn("fold -> {}", rdd.fold(0)(_ + _))
    rdd.foreach(println(_))


    val rdd2 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
    logger.warn("countByKey -> {}", rdd2.countByKey)
    rdd2.foreach(println(_))

  }


}
