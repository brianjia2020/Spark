package com.brianjia.bigdata.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO create SparkSQL running env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //TODO perform logical operation

    //DataFrame
//    val df: DataFrame = spark.read.json("data/user.json")
//    df.show()

    //DataFrame => SQL
//    df.createOrReplaceTempView("user")
//    spark.sql("select avg(age) from user").show
    //DataFrame => DSL
    //df.select("age","username").show()
    //df.select('age+1).show()


    //DataSet
    val seq = Seq(1,2,3,4)
//    val ds: Dataset[Int] = seq.toDS()
//    ds.show()

    //RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",40)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()

    //RDD <=> DataSet
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val rdd1: RDD[User] = ds1.rdd

    //TODO close env
    spark.close()
  }

  case class User(id: Int, name:String, age:Int)
}
