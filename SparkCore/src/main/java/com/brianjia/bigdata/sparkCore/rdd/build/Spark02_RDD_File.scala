package com.brianjia.bigdata.sparkCore.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO create the env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO create the rdd
    val rdd : RDD[String] = sc.textFile("data/1.txt")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
