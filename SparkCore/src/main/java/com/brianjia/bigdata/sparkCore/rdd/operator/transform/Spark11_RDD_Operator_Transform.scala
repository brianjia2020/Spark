package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - repartition
    // cause shuffling of data
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)

    val newRDD = rdd.repartition(3)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
