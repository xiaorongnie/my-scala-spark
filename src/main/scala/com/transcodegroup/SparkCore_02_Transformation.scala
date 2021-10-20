package com.transcodegroup

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.apache.log4j.{Level, Logger}

/**
 *
 * 在Spark中创建RDD的创建方式可以分为三种
 *
 * @author EASON
 */
object SparkCore_02_Transformation {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    //创建conf对象
    val conf = new SparkConf()
      .setAppName("SparkCore_02_Transformation")
      .setMaster("local")
    val sc = new SparkContext(conf)
    // 单value类型
    // simpleValueOpt(sc)

    // 双value操作
    doubleValueOpt(sc)

    // key-value操作
    keyValueOpt(sc)
  }

  /**
   * 单个value类型操作
   *
   * @param sc
   * @return
   */
  def simpleValueOpt(sc: SparkContext): Unit = {
    //要通过并行化集合方式创建RDD，那么就调用SparkContext以及其子类的parallelize()的方法
    val rdd1 = sc.parallelize(1 to 10, 5)
    val rdd2 = rdd1.map(_ * 2)
    logger.warn("transformation map -> {}", rdd2.collect)

    // 类似于map(func), 但是是独立在每个分区上运行.
    val rdd3 = rdd1.mapPartitions(it => it.map(_ * 2))
    logger.warn("transformation mapPartitions -> {}", rdd3.collect)

    // 使每个元素跟所在分区形成一个元组组成一个新的RDD
    val indexRdd = rdd1.mapPartitionsWithIndex((index, items) => (items.map((index, _))))
    logger.warn("transformation mapPartitionsWithIndex -> {}", indexRdd.collect)

    // {苹果，梨子}.flatMap(切碎) = {苹果碎片1，苹果碎片2，梨子碎片1，梨子碎片2}
    val flatMapRdd1 = rdd1.flatMap(1 to _)
    logger.warn("transformation flatMapRdd1 -> {}", flatMapRdd1.collect)

    // {苹果，梨子}.flatMap(切碎) = {苹果碎片1，苹果碎片2，梨子碎片1，梨子碎片2}
    val flatMapRdd2 = rdd1.flatMap(x => Array(x + 1, x * x, x * x * x))
    logger.warn("transformation flatMapRdd2 -> {}", flatMapRdd2.collect)

    // 将每一个分区的元素合并成一个数组，形成新的 RDD 类型是RDD[Array[T]]
    logger.warn("transformation glom -> {}", rdd1.collect)
    logger.warn("transformation glom -> {}", rdd1.glom.collect)

    // 分组1
    logger.warn("transformation groupBy1 -> {}", rdd1.groupBy(_ % 2).collect)
    logger.warn("transformation groupBy1 -> {}", rdd1.groupBy(x => if (x % 2 == 1) "odd" else "even").collect)

    // 过滤
    logger.warn("transformation filter -> {}", rdd1.filter(_ % 2 == 1).collect)

    // 抽样-放回抽样
    logger.warn("transformation sample -> {}", rdd1.sample(true,0.4,2).collect)
    logger.warn("transformation sample -> {}", rdd1.sample(false,0.2,3).collect)

    // 对 RDD 中元素执行去重操作. 参数表示任务的数量.默认值和分区数保持一致
    logger.warn("transformation distinct -> {}", rdd1.distinct().collect)

    // 缩减分区coalesce, coalesce重新分区，可以选择是否进行shuffle过程。由参数shuffle: Boolean = false/true决定。
    logger.warn("transformation coalesce -> {}", rdd1.coalesce(2).glom.collect)
    logger.warn("transformation coalesce -> {}", rdd1.repartition(2).glom.collect)

    // 排序
    logger.warn("transformation sortBy -> {}", rdd1.sortBy(x => x%3).collect)
  }

  /**
   * 双value类型操作
   */
  def doubleValueOpt(sc:SparkContext): Unit = {

    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(3 to 5)

    // union 求并集. 对源 RDD 和参数 RDD 求并集后返回一个新的 RDD
    logger.warn("transformation union -> {}", rdd1.union(rdd2).collect)

    // subtract 计算差集. 从原 RDD 中减去 原 RDD 和 otherDataset 中的共同的部分
    logger.warn("transformation subtract -> {}", rdd1.subtract(rdd2).collect)

    // intersection 对源RDD和参数RDD求交集后返回一个新的RDD
    logger.warn("transformation intersection -> {}", rdd1.intersection(rdd2).collect)

    // cartesian 计算 2 个 RDD 的笛卡尔积. 尽量避免使用 x*y条记录
    logger.warn("transformation intersection -> {}", rdd1.cartesian(rdd2).collect)

    // 拉链式操作, 分区和数量必选相同
    logger.warn("transformation intersection -> {}", rdd1.zip(rdd2).collect)
  }


  /**
   * keyValueOpt操作
   */
  def keyValueOpt(sc:SparkContext): Unit  = {
    val rdd1 = sc.parallelize(List((1,"aaa"),(2,"bbb"),(3,"ccc"),(1,"ddd")),4)
    logger.warn("transformation HashPartitioner1 -> {}", rdd1.glom.collect)
    // 对RDD重新分区
    var rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
    logger.warn("transformation HashPartitioner2 -> {}", rdd2.glom().collect)

    // groupByKey按特殊字段分组
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
    logger.warn("transformation wordPairsRDD -> {}", wordPairsRDD.glom().collect)
    val group = wordPairsRDD.groupByKey()
    logger.warn("transformation group -> {}", group.glom().collect)
    logger.warn("transformation group sum -> {}", group.map(x => (x._1, x._2.sum)).collect())

    // reduceByKey 相同key的value聚合到一起reduceByKey比groupByKey性能更好，建议使用。但是需要注意是否会影响业务逻辑。
    logger.warn("transformation reduceByKey -> {}", rdd1.reduceByKey((x,y) => x+y).collect())

    // aggregateByKey 创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    val agg = rdd.aggregateByKey(0)(math.max(_,_),_+_)
    logger.warn("transformation aggregateByKey -> {}", agg.collect())

    // foldByKey aggregateByKey的简化操作，seqop和combop相同
    logger.warn("transformation foldByKey -> {}", rdd.foldByKey(0)(_+_).collect())

    // combineByKey 对相同K，把V合并成一个集合。将相同key对应的值相加，同时记录该key出现的次数，放入一个二元组,然后求平均
    // 1.创建一个pairRDD
    val input = sc.parallelize(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),3)
    logger.warn("transformation combine -> {}", input.glom().collect)
    // 2. 将相同key对应的值相加，同时记录该key出现的次数，放入一个二元组
    val combine1 = input.combineByKey((_,1), (acc:(Int,Int),v)=>(acc._1+v,acc._2+1), (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    // 3.打印合并后的结果
    logger.warn("transformation combine -> {}", combine1.collect)

    // sortByKey
    val sortByKeyRdd = sc.parallelize(List((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    logger.warn("transformation sortByKeyRdd ASC -> {}", sortByKeyRdd.sortByKey().collect)
    logger.warn("transformation sortByKeyRdd DESC -> {}", sortByKeyRdd.sortByKey(false).collect)

    // mapValues 针对于(K,V)形式的类型只对V进行操作
    val rdd3 = sc.parallelize(List((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    logger.warn("transformation sortByKeyRdd DESC -> {}", rdd3.mapValues(_+"HAHA").collect())

    // join 内连接:在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
    val joinRdd1 = sc.parallelize(List((1,1),(2,2),(3,'a'),(3,'b')))
    val joinRdd2 = sc.parallelize(List((1,4),(2,5),(3,3)))
    logger.warn("transformation join -> {}", joinRdd1.join(joinRdd2).collect())

    // cogroup在聚合时会先对RDD中相同的key进行合并。
    logger.warn("transformation cogroup -> {}", joinRdd1.cogroup(joinRdd2).collect())
  }
}
