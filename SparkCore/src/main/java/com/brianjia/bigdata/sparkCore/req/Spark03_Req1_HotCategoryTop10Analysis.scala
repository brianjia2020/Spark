package com.brianjia.bigdata.sparkCore.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategory")
    val sc = new SparkContext(sparkConf)

    //Issue: too many shuffle operation, reduceByKey

    //1. load raw data
    val actionRDD = sc.textFile("/Users/chunyangjia/Desktop/jars/studyMaterial/Spark3/material/spark-core-data/user_visit_action.txt")
    //2. transform the structure to
    //   click (cid, (1,0,0))
    //   order (cid, (0,1,0))
    //   payment (cid, (0,0,1))
    val flatRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    //3. aggregation based on cid
    //   (cid, (cnt,cnt,cnt))
    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    //4. order by and collect
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)
    sc.stop()
  }
}
