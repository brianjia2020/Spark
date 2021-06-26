package com.brianjia.bigdata.sparkCore.framework.service


import com.brianjia.bigdata.sparkCore.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService {
  private val wordCountDao = new WordCountDao();
  def dataAnalysis() = {
    //TODO business logic
    //1. read the source, get the line content

    //2. break each line into individual words
    val lines: RDD[String] = wordCountDao.readFile("/Users/chunyangjia/IdeaProjects/Spark/data")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3. count the number of words
    val wordGroup:RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordToCount: RDD[(String, Int)] = wordGroup.map{
      case(word, list) => {
        (word,list.size)
      }
    }

    //4. print to console
    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples
  }
}
