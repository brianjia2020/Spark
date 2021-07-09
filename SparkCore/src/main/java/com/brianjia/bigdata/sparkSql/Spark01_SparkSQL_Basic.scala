package com.brianjia.bigdata.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO create SparkSQL running env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //TODO perform logical operation

    //DataFrame
    val df: DataFrame = spark.read.json("data/user.json")
//    df.show()

    //DataFrame => SQL
    df.createOrReplaceTempView("user")
    spark.sql("select avg(age) from user").show
    //DataFrame => DSL
    df.select("age","username").show()
    import spark.implicits._
    df.select('age+1).show()


    //DataSet


    //TODO close env
    spark.close()

  }
}
