package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - Join

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))

    val rdd2 = sc.makeRDD(List(
      ("a",4),("b",5),("c",6),("d",1)
    ))

    rdd1.join(rdd2).collect().foreach(println)
    
    sc.stop()
  }
}
