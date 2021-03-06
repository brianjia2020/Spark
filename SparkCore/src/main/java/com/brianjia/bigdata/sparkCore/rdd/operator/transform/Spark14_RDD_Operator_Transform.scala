package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - reduceByKey

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4)
    ))

    rdd.reduceByKey((x:Int,y:Int) => {x+y}).collect().foreach(println)

    sc.stop()
  }
}
