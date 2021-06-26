package com.brianjia.bigdata.sparkCore.framework.common

import com.brianjia.bigdata.sparkCore.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start(master: String = "local[*]", app: String = "BrianApp")( op: => Unit) = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)

    try {
      op
    }catch {
      case ex => println(ex.getMessage)
    }
    //TODO close the links
    sc.stop()
  }
}
