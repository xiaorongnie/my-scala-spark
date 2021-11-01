package com.transcodegroup.sparkcore

import lombok.Data

@Data
object HelloWord {
  def main(args: Array[String]): Unit = {
    println("file:///E:/aa/bb/cc.txt")
    val list = Array.range(1, 10)

    // partition
    println("partition...")
    val partition = list.partition(_ < 5)
    println(partition._1.mkString(","))
    println(partition._2.mkString(","))

    // groupBy
    println("groupBy...")
    val groupBy = list.groupBy(_ % 3)
    println(groupBy.getClass)
    for (element <- groupBy) {
      println(element._2.mkString(","))
    }
    // grouped
    println("grouped...")
    val grouped = list.grouped(5)
    println(grouped.getClass)
    while (grouped.hasNext) {
      println(grouped.next().mkString(","))
    }

    // sliding
    println("sliding...")
    val sliding = list.sliding(5)
    println(sliding.getClass)
    while (sliding.hasNext) {
      println(sliding.next().mkString(","))
    }
  }
}
