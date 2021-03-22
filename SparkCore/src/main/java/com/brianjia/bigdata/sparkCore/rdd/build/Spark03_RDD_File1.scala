package com.brianjia.bigdata.sparkCore.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //TODO create the env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO create the rdd
    val rdd : RDD[(String,String)] = sc.wholeTextFiles("data")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
