package com.brianjia.bigdata.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}


object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //TODO create SparkSQL running env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //TODO perform logical operation

    spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "localhost")
      .option("password", "Duanxiaohong1966")
      .option("dbtable", "user")
      .load().show()

    //TODO close env
    spark.close()
  }
}
