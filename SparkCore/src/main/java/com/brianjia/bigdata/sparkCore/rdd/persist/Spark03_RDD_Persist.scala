package com.brianjia.bigdata.sparkCore.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")
    val rdd = sc.makeRDD(list)
    val flatRdd = rdd.flatMap(_.split(" "))
    val mapRDD = flatRdd.map(word => {
      println("@@@@@@@@@@")
      (word,1)
    })
    //checkpoint is to fall onto the disk and has set save path
    //checkpoint is after the check, it won't be saved
    //usually
    mapRDD.checkpoint()
    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)
    println("******************")
    //rdd does not store data, only dependencies
    //so the calculation has been performed twice
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
