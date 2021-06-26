package com.brianjia.bigdata.sparkCore.framework.application

import com.brianjia.bigdata.sparkCore.framework.common.TApplication
import com.brianjia.bigdata.sparkCore.framework.controller.WordCountController
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplication extends App with TApplication{

  //Application
  //Spark framework
  //TODO build spark frame links
  //JDBC : Connection
  start(){
    val controller = new WordCountController()
    controller.dispatch()
  }

}
