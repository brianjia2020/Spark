package com.brianjia.bigdata.sparkCore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",1),("a",1)
    ))

    //TODO - aggregate
//    val result = rdd.countByValue()
    val result = rdd.countByKey()
    println(result)
    sc.stop()
  }

}
