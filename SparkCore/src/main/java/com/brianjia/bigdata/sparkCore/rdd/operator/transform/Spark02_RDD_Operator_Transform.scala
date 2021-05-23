package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO operator - mapPartitions
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //1. mapPartitions is a in-memory processing
    //mapPartitions : can transform data based on each partition
    //                but will load all partition data in memory
    //                after transformation, the data will not be released
    //                Hence, when data size is big and memory is small, use map
    //                Otherwise use mapPartitions
    val mpRDD = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>")
        iter.map(_*2)
      }
    )

    mpRDD.collect().foreach(println)

    sc.stop()
  }
}
