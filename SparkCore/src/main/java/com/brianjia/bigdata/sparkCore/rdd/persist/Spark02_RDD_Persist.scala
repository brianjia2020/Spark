package com.brianjia.bigdata.sparkCore.rdd.persist

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")
    val rdd = sc.makeRDD(list)
    val flatRdd = rdd.flatMap(_.split(" "))
    val mapRDD = flatRdd.map((_,1))
//    mapRDD.cache()
    mapRDD.persist(StorageLevel.DISK_ONLY)
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
