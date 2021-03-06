package com.brianjia.bigdata.sparkCore.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO create the env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO create the rdd
    //textFile can read in files and use default partitions
    //      minPartitions:
    //      math.min(defaultParallelism, 2)
    //      you can also specify the partitions
    //totalSize = number of bytes
    //goalSize = 7/2 =3 bytes
    val rdd : RDD[String] = sc.textFile("data/1.txt",3)
    rdd.collect().foreach(println)
//    rdd.saveAsTextFile("output/")

    //TODO close the env
    sc.stop()
  }
}
