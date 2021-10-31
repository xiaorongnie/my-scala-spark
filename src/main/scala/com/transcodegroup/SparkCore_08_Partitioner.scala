package com.transcodegroup

import com.transcodegroup.SparkCore_03_Action.logger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 *
 * Spark Core快速入门系列(10) | Key-Value 类型 RDD 的数据分区器
 *
 * 对于只存储 value的 RDD, 不需要分区器.
 *
 * 只有存储Key-Value类型的才会需要分区器.
 *
 * Spark 目前支持 Hash 分区和 Range 分区，用户也可以自定义分区.
 *
 * Hash 分区为当前的默认分区，Spark 中分区器直接决定了 RDD 中分区的个数、RDD 中每条数据经过 Shuffle 过程后属于哪个分区和 Reduce 的个数.
 *
 * @author EASON
 */
object SparkCore_08_Partitioner {

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

    val rdd1 = sc.parallelize(Array(10))
    logger.warn("{} -> {}", rdd1.collect(), rdd1.partitioner)

    val rdd2 = sc.parallelize(List(("hello", 1), ("world", 1), ("world", 2)))
    logger.warn("{} -> {}", rdd2.glom.collect, rdd2.partitioner)

    val rdd3 = rdd2.partitionBy(new HashPartitioner(3))
    logger.warn("{} -> {}", rdd3.glom.collect(), rdd3.partitioner)
    hashPartition(sc)
    rangePartitioner(sc)
  }

  /**
   * HashPartitioner 分区弊端： 可能导致每个分区中数据量的不均匀，极端情况下会导致某些分区拥有 RDD 的全部数据。比如我们前面的例子就是一个极端, 他们都进入了 0 分区.
   * @param sc
   */
  def hashPartition(sc: SparkContext): Unit = {
    val rdd11 = sc.parallelize(List((10, "a"), (20, "b"), (30, "c"), (40, "d"), (50, "e"), (61, "f")))
    // 把分区号取出来, 检查元素的分区情况
    val rdd12: RDD[(Int, String)] = rdd11.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))

    println(rdd12.collect.mkString(","))

    // 把 RDD1使用 HashPartitioner重新分区
    val rdd13 = rdd11.partitionBy(new HashPartitioner(5))
    // 检测RDD3的分区情况
    val rdd14: RDD[(Int, String)] = rdd13.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))
    println(rdd14.collect.mkString(","))
  }

  /**
   *  RangePartitioner 作用：将一定范围内的数映射到某一个分区内，尽量保证每个分区中数据量的均匀，而且分区与分区之间是有序的，
   *  一个分区中的元素肯定都是比另一个分区内的元素小或者大，但是分区内的元素是不能保证顺序的。简单的说就是将一定范围内的数映射到某一个分区内。实现过程为：
   * @param sc
   */
  def rangePartitioner(sc: SparkContext): Unit = {
    val rdd11 = sc.parallelize(List((10, "a"), (30, "c"), (20, "b"), (40, "d"), (50, "e"), (61, "f")))
    // 把分区号取出来, 检查元素的分区情况
    val rdd12: RDD[(Int, String)] = rdd11.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))

    println(rdd12.collect.mkString(","))

    // 把 RDD1使用 HashPartitioner重新分区
    val rdd13 = rdd11.partitionBy(new RangePartitioner(3, rdd11))
    // 检测RDD3的分区情况
    val rdd14: RDD[(Int, String)] = rdd13.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + " : " + x._2)))
    println(rdd14.collect.mkString(","))
  }
}