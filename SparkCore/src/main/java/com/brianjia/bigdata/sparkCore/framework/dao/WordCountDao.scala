package com.brianjia.bigdata.sparkCore.framework.dao

import com.brianjia.bigdata.sparkCore.framework.application.WordCountApplication.sc
import org.apache.spark.rdd.RDD

/**
 *
 */
class WordCountDao {
  def readFile(path: String) = {
    val lines: RDD[String] = sc.textFile(path)
    lines
  }
}
