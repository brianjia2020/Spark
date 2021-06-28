package com.brianjia.bigdata.sparkCore.framework.controller

import com.brianjia.bigdata.sparkCore.framework.common.TController
import com.brianjia.bigdata.sparkCore.framework.service.WordCountService
import org.apache.spark.rdd.RDD

/**
 * controller layer
 */
class WordCountController extends TController{
  private val wordCountService = new WordCountService()

  override def dispatch(): Unit = {
    val tuples = wordCountService.dataAnalysis()
    tuples.foreach(println)
  }
}
