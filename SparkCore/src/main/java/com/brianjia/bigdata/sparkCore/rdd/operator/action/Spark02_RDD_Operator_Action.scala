package com.brianjia.bigdata.sparkCore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - reduce
    //reduce
    val i = rdd.reduce(_ + _)
    println(i)
    //collect
    val ints = rdd.collect()
    println(ints.mkString(","))
    //count
    val count = rdd.count()
    println(count)
    //first
    val first = rdd.first()
    println(first)
    //take
    val take = rdd.take(3)
    println(take.mkString(","))
    //takeOrdered
    val take2 = rdd.takeOrdered(3)
    println(take2.mkString(","))
    sc.stop()
  }

}
