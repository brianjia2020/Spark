package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - sortBy(cause shuffling)
    // cause shuffling of data
    // coalesce is used to decrease partitions
    // repartition: is used to increase partitions (equal to coalesce and shuffle to be true)
    val rdd:RDD[Int] = sc.makeRDD(List(6,2,4,5,3,1),2)
    val sortRDD: RDD[Int]= rdd.sortBy(num => num)
    sortRDD.saveAsTextFile("output")
    sc.stop()
  }
}
