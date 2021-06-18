package com.brianjia.bigdata.sparkCore.rdd.partition

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_part {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxx"),
      ("cba", "xxxxxxxx"),
      ("wnba", "xxxxxxxx"),
      ("nba", "xxxxxxxx")
    ))
    val partRDD = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  /**
   * self-defined partitioner
   * 1. extends Partitioner class
   * 2. overload the function
   */
  class MyPartitioner extends Partitioner{
    //number of partitions
    override def numPartitions: Int = 3

    //return data partition index
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
