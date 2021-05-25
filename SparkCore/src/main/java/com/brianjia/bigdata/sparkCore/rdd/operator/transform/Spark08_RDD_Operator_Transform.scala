package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - sample
    // cause shuffling of data
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    //sample
    //  1. withReplacement: if will place back the data
    //  2. ratio of each data being drawn
    //  3. random seed, if passed data will be fixed else it will be random
    println(rdd.sample(
      false,
      0.4,
      1
    ).collect().mkString(","))
    sc.stop()
  }
}
