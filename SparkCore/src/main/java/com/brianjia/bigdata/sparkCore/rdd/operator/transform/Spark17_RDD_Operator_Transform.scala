package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - aggregateByKey (Key,Value)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("a",4)
    ),2)

    //(a, [1,2]),(a,[3,4])
    //aggregateByKey has two variable list
    //    1.the first one - initial value (a,0),(b,0) to compare to the very first set
    //    2.the second one - inside each partition calculation,between each partition


    rdd.aggregateByKey(0)(
      //the calculation inside each partition
      (x,y) => math.max(x,y),
      //the calculation between each partition
      (x,y) => x + y
    ).collect().foreach(println)
    
    sc.stop()
  }
}
