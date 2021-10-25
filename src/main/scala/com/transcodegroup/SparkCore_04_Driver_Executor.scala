package com.transcodegroup

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD

/**
 *
 * 我们进行 Spark 进行编程的时候, 初始化工作是在 driver端完成的, 而实际的运行程序是在executor端进行的. 所以就涉及到了进程间的通讯, 数据是需要序列化的.
 *
 * 1. var q = query 报错的主要原因是Executor端 拿不到需要变量、或者方法，所以需要序列化传送，就有了把传进来的变量赋值给方法内部的变量这样的操作，下面还有另外的操作。
 * 2. Serializable 因为 装饰者模式 的存在，自己封装的操作 只需要继承特质Serializable就好，上面的 传递变量也可以这样。
 * 3. Spark 出于性能的考虑, 支持另外一种序列化机制: kryo (2.0开始支持). kryo 比较快和简洁.(速度是Serializable的10倍). 想获取更好的性能应该使用 kryo 来序列化.
 *
 * @author EASON
 */
object SparkCore_04_Driver_Executor {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //创建conf对象
    val conf = new SparkConf()
      .setAppName("SparkCore_03_Action")
      .setMaster("local")
      // Spark 出于性能的考虑, 支持另外一种序列化机制: kryo (2.0开始支持). kryo 比较快和简洁.(速度是Serializable的10倍). 想获取更好的性能应该使用 kryo 来序列化.
      // 替换默认的序列化机制 可以省(如果调用registerKryoClasses
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
      .registerKryoClasses(Array(classOf[Searcher1], classOf[Searcher2]))
    val sc = new SparkContext(conf)
    // 在 RDD 中查找出来包含 query 子字符串的元素
    searcher1(sc)
    searcher2(sc)
    searcher3(sc)
  }

  /**
   * 在 RDD 中查找出来包含 query 子字符串的元素
   *
   * @param sc
   * @return
   */
  def searcher1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello buwenbuhuo", "xia0li", "hahah"), 2)
    val searcher = new Searcher1("hello")
    // 创建传递函数
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)
    result.collect.foreach(println)
  }

  // 创建的类
  // query 为需要查找的子字符串
  class Searcher1(val query: String) extends Serializable {

    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String) = {
      s.contains(query)
    }

    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch) //
    }

    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD2(rdd: RDD[String]) = {
      rdd.filter(_.contains(query))
    }
  }

  /**
   * 在 RDD 中查找出来包含 query 子字符串的元素
   *
   * @param sc
   * @return
   */
  def searcher2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.parallelize(Array("hello world 2", "hello buwenbuhuo 2", "xia0li", "hahah"), 2)

    val searcher = new Searcher2("hello")
    val result: RDD[String] = searcher.getMatchedRDD2(rdd)
    result.collect.foreach(println)
  }

  // 创建的类
  // query 为需要查找的子字符串
  class Searcher2(val query: String) extends Serializable {

    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String) = {
      s.contains(query)
    }

    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch) //
    }

    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD2(rdd: RDD[String]) = {
      rdd.filter(_.contains(query))
    }
  }


  /**
   * 在 RDD 中查找出来包含 query 子字符串的元素
   *
   * @param sc
   * @return
   */
  def searcher3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.parallelize(Array("hello world 3", "hello buwenbuhuo 3", "xia0li", "hahah"), 2)

    val searcher = new Searcher3("hello")
    val result: RDD[String] = searcher.getMatchedRDD2(rdd)
    result.collect.foreach(println)
  }

  // 创建的类
  // query 为需要查找的子字符串
  class Searcher3(val query: String) {

    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String) = {
      s.contains(query)
    }

    // 过滤出包含 query字符串的字符串组成的新的 RDD
    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch) //
    }

    // 传递局部变量而不是属性
    def getMatchedRDD2(rdd: RDD[String]) = {
      // rdd.filter(_.contains(query))
      var q = query
      rdd.filter(_.contains(q))
    }
  }

}
