package com.brianjia.bigdata.sparkCore.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    //cache: temporarily store data in the memory, not very safe
    //persist: temporarily store data in the disk or mem as specified
    //         may involve disk IO, slow but safe
    //         after the job has been done, the data will be deleted
    //checkpoint: permanently store the data in the disk
    //            to ensure data security, will perform the task separately
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
