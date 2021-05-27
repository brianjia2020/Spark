package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - partitionBy(Key,Value)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4)
    ))


    val groupRDD = rdd.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
