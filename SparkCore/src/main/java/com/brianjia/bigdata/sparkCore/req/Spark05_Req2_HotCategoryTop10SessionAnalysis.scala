package com.brianjia.bigdata.sparkCore.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategory")
    val sc = new SparkContext(sparkConf)

    val actionRDD = sc.textFile("/Users/chunyangjia/Desktop/jars/studyMaterial/Spark3/material/spark-core-data/user_visit_action.txt")
    actionRDD.cache()
    val top10Ids: Array[String] = top10Category(actionRDD)

    //1. filter raw data, only keep the click and top 10 ids
    val filterActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )

    //2. based on cid and sessionId to summarize click data
    val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    //3. structure transformation
    // ((cid, sessionId), sum)
    val mapRDD = reduceRDD.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    //4. similar cid will be grouped into one group
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    //5. sort by click data and take top 10 for each cid
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    resultRDD.collect().foreach(println)

    sc.stop()
  }

  def top10Category(actionRDD: RDD[String]) = {
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
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10).map(_._1)
    resultRDD
  }
}
