package com.brianjia.bigdata.sparkCore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - action operator

    //action operators can trigger a job
    //       sc.runJob
    //       ActiveJob then submit job
    rdd.collect()

    sc.stop()
  }

}
