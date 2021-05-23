package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Parallel {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - map
    //if there is only one partition, the data will be processed one by one in a linear way
    //however since this mac has 6 cores, default partition is like 6 and parallel process
    //will be performed simultaneously and the order of output will not be 1,2,3,4 but in
    //a random way

    //inside each partition, it's ordered
    //between partitions, it's without order
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD = rdd.map(
      num => {
        println(">>>>" + num)
        num
      }
    )

    val mapRDD1 = mapRDD.map(
      num => {
        println("####" + num)
        num
      }
    )

    mapRDD1.collect()
    
    sc.stop()
  }
}
