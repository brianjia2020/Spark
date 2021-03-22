package com.brianjia.bigdata.sparkCore.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO prepare the env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO create the rdd
    //from memory, treating the in-memory data as source
    val seq = Seq[Int](1,2,3,4)
//    val rdd : RDD[Int] = sc.parallelize(seq)
    val rdd : RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(print)

    //TODO close the env
    sc.stop()
  }
}
