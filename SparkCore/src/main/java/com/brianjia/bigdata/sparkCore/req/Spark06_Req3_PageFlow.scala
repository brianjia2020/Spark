package com.brianjia.bigdata.sparkCore.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageFlow {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategory")
    val sc = new SparkContext(sparkConf)

    val actionRDD = sc.textFile("/Users/chunyangjia/Desktop/jars/studyMaterial/Spark3/material/spark-core-data/user_visit_action.txt")

    var actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    //TODO calculate only targeted page turning rate
    //     help page to main page, we don't care
    val ids = List(1,2,3,4,5,6,7)

    //TODO calculate total click data
    val pageIdToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    //TODO calculate pay data
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    //for each group, sort By timestamp (ascending)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        //[1,2,3,4] => [1-2,2-3,3-4]
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageflowIds.filter(
          t => {
            ids.contains(t._1) & ids.contains(t._2)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )

    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

    //TODO calculate the turning ratio
    dataRDD.foreach{
      case ((pageid1,pageid2) , sum ) => {
        val long = pageIdToCountMap.getOrElse(pageid1, 0L)
        println(s"page ${pageid1} to page ${pageid2} turning ratio is: " + (sum.toDouble/long))
      }
    }
    sc.stop()
  }

  case class UserVisitAction (
     date: String,
     user_id: Long,
     session_id: String,
     page_id: Long,
     action_time: String,
     search_keyword: String,
     click_category_id: Long,
     click_product_id: Long,
     order_category_ids: String,
     order_product_ids: String,
     pay_category_ids: String,
     pay_product_ids: String,
     city_id: Long
     )
}
