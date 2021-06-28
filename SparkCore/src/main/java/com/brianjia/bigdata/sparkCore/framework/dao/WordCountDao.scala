package com.brianjia.bigdata.sparkCore.framework.dao

import com.brianjia.bigdata.sparkCore.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 *
 */
class WordCountDao {
  def readFile(path: String) = {
    val lines: RDD[String] = EnvUtil.take().textFile(path)
    lines
  }
}
