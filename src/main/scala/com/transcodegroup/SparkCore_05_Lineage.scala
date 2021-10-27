package com.transcodegroup

import com.transcodegroup.SparkCore_02_Transformation.logger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 *
 * 只支持粗粒度转换，即在大量记录上执行的单个操作。将创建RDD的一系列Lineage（血统）记录下来，以便恢复丢失的分区。
 *
 * RDD的Lineage会记录RDD的元数据信息和转换行为，当该RDD的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的数据分区。
 *
 * @author EASON
 */
object SparkCore_05_Lineage {

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
    lineage(sc)
  }

  /**
   * 在 RDD 中查找出来包含 query 子字符串的元素
   *
   * @param sc
   * @return
   */
  def lineage(sc: SparkContext): Unit = {
    val wordAndOne = sc.textFile("file:///C:/Users/eason/IdeaProjects/my-scala-spark/data/words.txt").flatMap(_.split("\t")).map((_,1))
    logger.warn("wordAndOne1 -> {}", wordAndOne.collect())
    logger.warn("wordAndOne2 -> {}", wordAndOne.toDebugString)
    logger.warn("wordAndOne3 -> {}",  wordAndOne.dependencies)

    val wordAndCount = wordAndOne.reduceByKey(_+_)
    logger.warn("wordAndCount1 -> {}", wordAndCount.collect())
    logger.warn("wordAndCount2 -> {}", wordAndCount.toDebugString)
    logger.warn("wordAndCount3 -> {}",  wordAndCount.dependencies)

  }
}