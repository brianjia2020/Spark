package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO operator - mapPartitions
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mpRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )

    mpRDD.collect().foreach(println)

    sc.stop()
  }
}
