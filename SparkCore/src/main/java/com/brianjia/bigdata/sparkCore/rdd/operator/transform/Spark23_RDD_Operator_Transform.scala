package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - cogroup

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))

    val rdd2 = sc.makeRDD(List(
      ("a",4),("b",5)
    ))

    //cogroup: connect + group
    val cgRDD = rdd1.cogroup(rdd2)
    cgRDD.collect().foreach(println)
    
    sc.stop()
  }
}
