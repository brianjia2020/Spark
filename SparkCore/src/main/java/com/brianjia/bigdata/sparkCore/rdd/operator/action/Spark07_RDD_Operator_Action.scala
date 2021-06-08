package com.brianjia.bigdata.sparkCore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      1,2,3,4
    ))

    //TODO -
    val user = new User()
    rdd.foreach(
      num => {
        println("user age =" + (num + user.age))
      }
    )


    sc.stop()
  }

  class User extends Serializable {
    var age : Int = 30;
  }

}
