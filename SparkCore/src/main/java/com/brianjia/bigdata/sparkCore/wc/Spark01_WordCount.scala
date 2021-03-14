package com.brianjia.bigdata.sparkCore.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark framework
    //TODO build spark frame links
    //JDBC : Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("Brian")
    val sc = new SparkContext(sparkConf)

    //TODO business logic

    //1. read the source, get the line content
    //2.


    //TODO close the links
    sc.stop()
  }
}
