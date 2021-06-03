package com.brianjia.bigdata.sparkCore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    //TODO - aggregate
    //aggregateByKey: initial value will only in calculation inside partition
    //aggregate: initial value will be involved in the calculation between partitions
    val result = rdd.aggregate(0)(_ + _, _ + _)
    val result1 = rdd.fold(0)(_+_)
    println(result,result1)
    sc.stop()
  }

}
