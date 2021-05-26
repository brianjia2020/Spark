package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - double value type

    val rdd1 = sc.makeRDD(List(1,2,3,4),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),4)

    //intersection
    println(rdd1.intersection(rdd2).collect().mkString(","))
    //union
    println(rdd1.union(rdd2).collect().mkString(","))
    //substract
    println(rdd1.subtract(rdd2).collect().mkString(","))
    //zip
    println(rdd1.repartition(4).zip(rdd2).collect().mkString(","))
    sc.stop()
  }
}
