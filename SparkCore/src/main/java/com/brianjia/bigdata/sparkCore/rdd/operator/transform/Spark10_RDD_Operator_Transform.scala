package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - coalesce
    // cause shuffling of data
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)


    //coalesce by default will not reorder the data
    //         1,2 and 3,4,5,6 will be the final output, this may cause data unbalanced
    //if we want to balance data, we can use shuffle to re-balance
    //if shuffle true, then will be 1,4,5 and 2,6,3
    //coalesce can be used to increase partition number and need to set shuffle as true
    val newRDD = rdd.coalesce(2,true)
    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
