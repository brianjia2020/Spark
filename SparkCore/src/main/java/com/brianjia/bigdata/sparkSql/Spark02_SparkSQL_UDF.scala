package com.brianjia.bigdata.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO create SparkSQL running env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //TODO perform logical operation

    val df = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName", (name:String) => {
      "Name: " + name
    })

    spark.sql("select age, prefixName(username) from user").show()

    //TODO close env
    spark.close()
  }

  case class User(id: Int, name:String, age:Int)
}
